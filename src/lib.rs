use std::sync::atomic::{AtomicI32, Ordering};

use pgrx::{
    AssertPGRXSharedMemory, PgAtomic, PgLwLock,
    bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags},
    pg_shmem_init,
    prelude::*,
};

use crate::{
    config::ExporterConfig,
    postgres::{collect_spans, request_instrumentation},
    span::HeaplessSpan,
    worker::background_worker_run,
};

mod config;
mod postgres;
mod sanitize;
mod span;
mod worker;

::pgrx::pg_module_magic!(name, version);

static DEQUE: PgLwLock<AssertPGRXSharedMemory<heapless::spsc::Queue<HeaplessSpan, 1024>>> =
    unsafe { PgLwLock::new(c"pg_otel_worker_deque") };

// The PID is published by the worker after it starts. A zero value means that
// the worker has not started (or has already exited).
static WORKER_PID: PgAtomic<AtomicI32> = unsafe { PgAtomic::new(c"pg_otel_worker_pid") };

// This is a global variable accross all plugins. We store the previous hook so we can execute it before our
// own hook. This is important because we want to make sure that the previous hook is executed before our own
// hook, so that we don't break any existing functionality.
static mut PREV_EXECUTOR_START: pg_sys::ExecutorStart_hook_type = None;
// Same, but for ExecutorEnd_hook
static mut PREV_EXECUTOR_END: pg_sys::ExecutorEnd_hook_type = None;

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    if unsafe { !pgrx::pg_sys::process_shared_preload_libraries_in_progress } {
        pgrx::error!("this extension must be loaded via shared_preload_libraries.");
    }
    ExporterConfig::define_gucs();
    reserve_guc_prefix();

    pg_shmem_init!(DEQUE = unsafe { AssertPGRXSharedMemory::new(Default::default()) });
    pg_shmem_init!(WORKER_PID);

    BackgroundWorkerBuilder::new("Background Worker Example")
        .set_function("background_worker_main")
        .set_library("pg_otel")
        .load();

    // SAFETY: This is called once by postgres
    unsafe {
        PREV_EXECUTOR_START = pg_sys::ExecutorStart_hook;
        pg_sys::ExecutorStart_hook = Some(my_executor_start_hook);
        PREV_EXECUTOR_END = pg_sys::ExecutorEnd_hook;
        pg_sys::ExecutorEnd_hook = Some(my_executor_end_hook);
    }
}

/// Rejects unknown `pg_otel.*` settings (typos in `postgresql.conf`) instead of
/// silently accepting them as placeholders.
fn reserve_guc_prefix() {
    // SAFETY: called from `_PG_init` after all `pg_otel.*` GUCs are defined.
    #[cfg(not(any(feature = "pg13", feature = "pg14")))]
    unsafe {
        pg_sys::MarkGUCPrefixReserved(c"pg_otel".as_ptr());
    }
    #[cfg(any(feature = "pg13", feature = "pg14"))]
    unsafe {
        pg_sys::EmitWarningsOnPlaceholders(c"pg_otel".as_ptr());
    }
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_main(_arg: pg_sys::Datum) {
    // Publish the PID from inside the worker process. MyProcPid is the PID that
    // another backend must signal to wake this worker.
    WORKER_PID
        .get()
        .swap(unsafe { pg_sys::MyProcPid }, Ordering::Relaxed);

    // these are the signals we want to receive.  If we don't attach the SIGTERM handler, then
    // we'll never be able to exit via an external notification
    BackgroundWorker::attach_signal_handlers(
        SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM | SignalWakeFlags::SIGINT,
    );

    background_worker_run();

    WORKER_PID.get().store(0, Ordering::Relaxed);

    log!(
        "Goodbye from inside the {} BGWorker! ",
        BackgroundWorker::get_name()
    );
}

#[pg_guard]
unsafe extern "C-unwind" fn my_executor_start_hook(
    query_desc: *mut pg_sys::QueryDesc,
    eflags: i32,
) {
    // Does nothing when tracing is disabled or in parallel workers.
    request_instrumentation(query_desc);

    // SAFETY: I am trusting the docs on this one.
    unsafe {
        if let Some(prev) = PREV_EXECUTOR_START {
            prev(query_desc, eflags);
        } else {
            pg_sys::standard_ExecutorStart(query_desc, eflags);
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn my_executor_end_hook(query_desc: *mut pg_sys::QueryDesc) {
    // Never raises: telemetry failures are contained so the previous / standard
    // ExecutorEnd below always runs. Parallel workers are skipped inside.
    collect_spans(query_desc);

    // SAFETY: I am trusting the docs on this one.
    unsafe {
        if let Some(prev) = PREV_EXECUTOR_END {
            prev(query_desc);
        } else {
            pg_sys::standard_ExecutorEnd(query_desc);
        }
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(any(test, feature = "pg_test"))]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // `_PG_init` refuses to run unless the library is preloaded.
        vec!["shared_preload_libraries = 'pg_otel'"]
    }
}
