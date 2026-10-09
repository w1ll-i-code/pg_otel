//! The span queue in Postgres shared memory.
//!
//! One chunk of shared memory holds a small header followed by the data area
//! of the byte ring ([`crate::queue`]). Its size is `pg_otel.queue_size_kb`,
//! which is only known at server start, so the chunk is requested and
//! initialised by hand in the shmem request/startup hooks instead of with
//! pgrx's `pg_shmem_init!` (which supports statically sized types only).
//!
//! # Concurrency
//!
//! * Backends (producers) and the exporter worker (consumer) serialise access
//!   to the ring with one named LWLock. The critical sections only copy bytes
//!   and never call into Postgres, because an ERROR raised (and caught) while
//!   holding an LWLock would leak it: Rust unwinding must not release it again
//!   after Postgres' own error cleanup, which is what [`LockGuard::drop`]
//!   checks for.
//! * The worker publishes its `ProcNumber` so backends can set its latch
//!   without signals; the other header fields are lock-free atomics.

use std::{
    cell::UnsafeCell,
    mem::size_of,
    ptr,
    sync::atomic::{AtomicI32, AtomicPtr, AtomicU64, Ordering},
};

use pgrx::{pg_guard, pg_sys};

use crate::{
    config::get_queue_size_kb,
    queue::{Batch, Corrupted, Ring, RingState, should_wake},
};

const SHMEM_NAME: &std::ffi::CStr = c"pg_otel_queue";
const LWLOCK_TRANCHE: &std::ffi::CStr = c"pg_otel_queue";
/// Index of `AddinShmemInitLock` in `MainLWLockArray` (see `lwlocklist.h`);
/// pgrx's own shared memory support relies on the same index.
const ADDIN_SHMEM_INIT_LOCK: usize = 21;
const NO_WORKER: i32 = -1;

#[repr(C)]
struct Header {
    /// Ring positions; only accessed with the queue lock held. The cell is what
    /// makes it legal to mutate this through a shared `&Header`.
    state: UnsafeCell<RingState>,
    /// Size of the data area following the header; never changes.
    capacity: u64,
    /// `ProcNumber` of the exporter worker, or [`NO_WORKER`].
    worker_proc_number: AtomicI32,
    /// Spans discarded because the queue was full (or a span was too large).
    dropped_spans: AtomicU64,
}

const HEADER_BYTES: usize = size_of::<Header>();

static HEADER: AtomicPtr<Header> = AtomicPtr::new(ptr::null_mut());

/// This process's pointer to the queue's LWLock. LWLock addresses are looked up
/// per process (`GetNamedLWLockTranche`) instead of being stored in shared
/// memory, where a pointer is only meaningful if every process maps it at the
/// same address.
static QUEUE_LOCK: AtomicPtr<pg_sys::LWLock> = AtomicPtr::new(ptr::null_mut());

static mut PREV_SHMEM_REQUEST_HOOK: pg_sys::shmem_request_hook_type = None;
static mut PREV_SHMEM_STARTUP_HOOK: pg_sys::shmem_startup_hook_type = None;

/// Bytes of shared memory needed for a queue of `queue_size_kb` kilobytes.
fn shared_memory_size(queue_size_kb: i32) -> usize {
    HEADER_BYTES + usize::try_from(queue_size_kb).unwrap_or(0) * 1024
}

/// Installs the shared memory hooks. Must be called from `_PG_init`, after the
/// GUCs are defined, while `shared_preload_libraries` is being processed.
pub fn install_hooks() {
    // SAFETY: called once from _PG_init in the postmaster.
    unsafe {
        PREV_SHMEM_REQUEST_HOOK = pg_sys::shmem_request_hook;
        pg_sys::shmem_request_hook = Some(on_shmem_request);
        PREV_SHMEM_STARTUP_HOOK = pg_sys::shmem_startup_hook;
        pg_sys::shmem_startup_hook = Some(on_shmem_startup);
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn on_shmem_request() {
    // SAFETY: hooks are only changed in _PG_init, before they can run.
    if let Some(prev) = unsafe { PREV_SHMEM_REQUEST_HOOK } {
        unsafe { prev() };
    }
    // SAFETY: allowed inside the shmem request hook.
    unsafe {
        pg_sys::RequestAddinShmemSpace(shared_memory_size(get_queue_size_kb()));
        pg_sys::RequestNamedLWLockTranche(LWLOCK_TRANCHE.as_ptr(), 1);
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn on_shmem_startup() {
    // SAFETY: see `on_shmem_request`.
    if let Some(prev) = unsafe { PREV_SHMEM_STARTUP_HOOK } {
        unsafe { prev() };
    }
    // SAFETY: inside the shmem startup hook.
    unsafe { init_shared_header() };
}

/// # Safety
///
/// Must be called from the shmem startup hook.
unsafe fn init_shared_header() {
    let size = shared_memory_size(get_queue_size_kb());
    // SAFETY: `ShmemInitStruct` must be called with AddinShmemInitLock held.
    unsafe {
        let init_lock = &raw mut (*pg_sys::MainLWLockArray.add(ADDIN_SHMEM_INIT_LOCK)).lock;
        pg_sys::LWLockAcquire(init_lock, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let mut found = false;
        let header =
            pg_sys::ShmemInitStruct(SHMEM_NAME.as_ptr(), size, &mut found).cast::<Header>();
        assert!(header.is_aligned(), "shared memory is not aligned");
        if !found {
            header.write(Header {
                state: UnsafeCell::new(RingState::new()),
                capacity: (size - HEADER_BYTES) as u64,
                worker_proc_number: AtomicI32::new(NO_WORKER),
                dropped_spans: AtomicU64::new(0),
            });
        }
        HEADER.store(header, Ordering::Release);
        QUEUE_LOCK.store(
            &raw mut (*pg_sys::GetNamedLWLockTranche(LWLOCK_TRANCHE.as_ptr())).lock,
            Ordering::Release,
        );

        pg_sys::LWLockRelease(init_lock);
    }
}

fn header() -> Option<&'static Header> {
    // SAFETY: set once in the startup hook to memory that lives until shutdown.
    unsafe { HEADER.load(Ordering::Acquire).as_ref() }
}

/// Holds the queue's LWLock.
struct LockGuard {
    lock: *mut pg_sys::LWLock,
}

impl LockGuard {
    /// Takes the queue lock, or `None` if shared memory was never set up.
    fn acquire() -> Option<Self> {
        let lock = QUEUE_LOCK.load(Ordering::Acquire);
        if lock.is_null() {
            return None;
        }
        // SAFETY: `lock` points to the valid named LWLock of this extension.
        unsafe { pg_sys::LWLockAcquire(lock, pg_sys::LWLockMode::LW_EXCLUSIVE) };
        Some(Self { lock })
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        // After an ERROR Postgres' cleanup has already released all LWLocks and
        // reset InterruptHoldoffCount to 0; releasing again would be a bug.
        // SAFETY: `lock` is valid; the holdoff count tells whether we still own it.
        unsafe {
            if pg_sys::InterruptHoldoffCount > 0 {
                pg_sys::LWLockRelease(self.lock);
            }
        }
    }
}

/// Runs `f` on the ring with the queue lock held.
///
/// `f` must not call into Postgres (see the module documentation). Returns
/// `None` when shared memory was never set up (the extension is not preloaded).
pub fn with_ring<R>(f: impl FnOnce(&mut Ring) -> R) -> Option<R> {
    let header = header()?;
    let _guard = LockGuard::acquire()?;
    // The data area is addressed through the raw pointer returned by
    // `ShmemInitStruct`, which (unlike a reference to the header) has
    // provenance over the whole allocation.
    let base = HEADER.load(Ordering::Acquire);
    // SAFETY: the lock gives exclusive access to the ring state and data area.
    // The state is behind an UnsafeCell, so no `&mut` aliases the shared
    // `&Header`; the data area directly follows the header, is `capacity` bytes
    // long and does not overlap it.
    let mut ring = unsafe {
        let data = std::slice::from_raw_parts_mut(
            base.cast::<u8>().add(HEADER_BYTES),
            header.capacity as usize,
        );
        Ring::new(&mut *header.state.get(), data)
    };
    Some(f(&mut ring))
}

/// Result of [`publish`].
#[derive(Debug, PartialEq, Eq)]
pub enum Published {
    /// The batch is queued.
    Queued,
    /// The batch was discarded (queue full, or no shared memory).
    Dropped,
}

/// Queues `batch` as a whole or drops it, counting the dropped spans, and wakes
/// the worker when that is useful (see [`should_wake`]).
///
/// Call without holding any lock; `skipped` is the number of spans that could
/// not even be encoded into `batch`.
pub fn publish(batch: &Batch, skipped: usize) -> Published {
    count_dropped(skipped);
    if batch.is_empty() {
        return Published::Queued;
    }
    let pushed = with_ring(|ring| {
        let used_before = ring.used();
        ring.push_batch(batch)
            .ok()
            .map(|()| should_wake(used_before, ring.used(), ring.capacity()))
    });
    match pushed {
        Some(Some(wake)) => {
            if wake {
                wake_worker();
            }
            Published::Queued
        }
        Some(None) | None => {
            count_dropped(batch.records());
            Published::Dropped
        }
    }
}

fn count_dropped(spans: usize) {
    if let Some(header) = header().filter(|_| spans > 0) {
        header
            .dropped_spans
            .fetch_add(spans as u64, Ordering::Relaxed);
    }
}

/// Total number of spans dropped since server start.
pub fn dropped_spans() -> u64 {
    header().map_or(0, |header| header.dropped_spans.load(Ordering::Relaxed))
}

/// Moves up to `max_bytes` of whole records into `out`; see
/// [`Ring::drain_into`]. Returns the number of records.
pub fn drain_into(out: &mut Vec<u8>, max_bytes: usize) -> Result<usize, Corrupted> {
    with_ring(|ring| ring.drain_into(out, max_bytes)).unwrap_or(Ok(0))
}

/// Records the calling process as the exporter worker, so backends can wake
/// it, and arranges for the registration to be removed when it exits (also
/// after an ERROR or FATAL, via `before_shmem_exit`).
pub fn register_worker() {
    let Some(header) = header() else { return };
    // SAFETY: `MyProcNumber` is valid in any process attached to shared memory.
    let proc_number = unsafe { pg_sys::MyProcNumber };
    header
        .worker_proc_number
        .store(proc_number, Ordering::Release);
    // SAFETY: the callback is a plain function.
    unsafe { pg_sys::before_shmem_exit(Some(unregister_worker_on_exit), pg_sys::Datum::from(0)) };
}

unsafe extern "C-unwind" fn unregister_worker_on_exit(_code: i32, _arg: pg_sys::Datum) {
    if let Some(header) = header() {
        header
            .worker_proc_number
            .store(NO_WORKER, Ordering::Release);
    }
}

/// Sets the worker's latch if a worker is registered.
///
/// A stale `ProcNumber` is harmless: setting the latch of a process slot that
/// is no longer (or newly) used only causes a spurious wake-up.
pub fn wake_worker() {
    let Some(header) = header() else { return };
    let proc_number = header.worker_proc_number.load(Ordering::Acquire);
    // SAFETY: `ProcGlobal` is set up before any backend runs; the index is
    // checked against the size of the PGPROC array. `SetLatch` is safe to call
    // from any process on a shared latch.
    unsafe {
        let globals = pg_sys::ProcGlobal;
        if proc_number < 0 || globals.is_null() || proc_number as u32 >= (*globals).allProcCount {
            return;
        }
        let proc = (*globals).allProcs.add(proc_number as usize);
        pg_sys::SetLatch(&raw mut (*proc).procLatch);
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn memory_size_includes_the_header() {
        assert_eq!(shared_memory_size(0), HEADER_BYTES);
        assert_eq!(shared_memory_size(64), HEADER_BYTES + 64 * 1024);
        assert_eq!(shared_memory_size(-5), HEADER_BYTES);
    }

    #[test]
    fn header_keeps_the_data_area_aligned() {
        assert_eq!(HEADER_BYTES % 8, 0);
    }
}

// The module must be called `tests`: pgrx looks `#[pg_test]` functions up in
// that schema.
#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::ffi::CStr;

    use pgrx::{pg_sys, prelude::*};

    use super::ADDIN_SHMEM_INIT_LOCK;

    #[pg_test]
    fn addin_shmem_init_lock_index_names_the_expected_lock() {
        // SAFETY: `MainLWLockArray` is initialised in every backend; the name
        // is a static string owned by Postgres.
        let name = unsafe {
            let lock = &raw const (*pg_sys::MainLWLockArray.add(ADDIN_SHMEM_INIT_LOCK)).lock;
            let tranche = (*lock).tranche;
            CStr::from_ptr(pg_sys::GetLWLockIdentifier(pg_sys::PG_WAIT_LWLOCK, tranche))
        };
        assert_eq!(name.to_str().unwrap(), "AddinShmemInit");
    }

    #[pg_test]
    fn queue_lock_is_resolved_for_this_process() {
        assert!(!super::QUEUE_LOCK.load(super::Ordering::Acquire).is_null());
        assert!(super::with_ring(|ring| ring.capacity()).is_some_and(|c| c >= 64 * 1024));
    }
}
