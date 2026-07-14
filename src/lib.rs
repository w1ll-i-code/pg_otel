use std::time::{Duration, SystemTime};

use pgrx::{
    bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags},
    pg_shmem_init,
    prelude::*,
    AssertPGRXSharedMemory, PgLwLock,
};

::pgrx::pg_module_magic!(name, version);

/// In order to use this bgworker with pgrx, you'll need to edit the proper `postgresql.conf` file in
/// "${PGRX_HOME}/data-$PGVER/postgresql.conf" and add this line to the end:
///
/// ```
/// shared_preload_libraries = 'pg_otel.so'
/// ```

#[derive(Debug)]
pub struct HeaplessSpan {
    pub trace_id: u64,
    pub span_id: u64,
    pub name: heapless::String<64>,
    pub start_time: SystemTime,
    pub end_time: SystemTime,
}

static DEQUE: PgLwLock<AssertPGRXSharedMemory<heapless::spsc::Queue<HeaplessSpan, 4096>>> =
    unsafe { PgLwLock::new(c"pg_otel_worker_deque") };

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

    pg_shmem_init!(DEQUE = unsafe { AssertPGRXSharedMemory::new(Default::default()) });

    BackgroundWorkerBuilder::new("Background Worker Example")
        .set_function("background_worker_main")
        .set_library("pg_otel")
        .set_argument(42i32.into_datum())
        .enable_spi_access()
        .load();

    // SAFETY: This is called once by postgres
    unsafe {
        PREV_EXECUTOR_START = pg_sys::ExecutorStart_hook;
        pg_sys::ExecutorStart_hook = Some(my_executor_start_hook);
        PREV_EXECUTOR_END = pg_sys::ExecutorEnd_hook;
        pg_sys::ExecutorEnd_hook = Some(my_executor_end_hook);
    }
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_main(_arg: pg_sys::Datum) {
    // these are the signals we want to receive.  If we don't attach the SIGTERM handler, then
    // we'll never be able to exit via an external notification
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    'outer: while BackgroundWorker::wait_latch(Some(Duration::from_millis(5000))) {
        if BackgroundWorker::sighup_received() {
            // on SIGHUP, you might want to reload some external configuration or something
        }

        loop {
            if BackgroundWorker::sigterm_received() {
                break 'outer;
            }

            match DEQUE.exclusive().dequeue() {
                Some(span) => {
                    log!("Dequeued span: {:?}", span);
                }
                None => {
                    log!("No more spans to dequeue");
                    break;
                }
            }
        }
    }

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
    if let Some(prev) = PREV_EXECUTOR_START {
        prev(query_desc, eflags);
    } else {
        pg_sys::standard_ExecutorStart(query_desc, eflags);
    }

    let result = DEQUE.exclusive().enqueue(HeaplessSpan {
        trace_id: 0,
        span_id: 0,
        name: "Hello, World!".try_into().unwrap(),
        start_time: SystemTime::now(),
        end_time: SystemTime::now(),
    });

    match result {
        Ok(_) => log!("Enqueued span"),
        Err(e) => error!("Failed to enqueue span: {:?}", e),
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn my_executor_end_hook(query_desc: *mut pg_sys::QueryDesc) {
    // Chain to previous hook first
    if let Some(prev) = PREV_EXECUTOR_END {
        prev(query_desc);
    } else {
        pg_sys::standard_ExecutorEnd(query_desc);
    }

    let result = DEQUE.exclusive().enqueue(HeaplessSpan {
        trace_id: 0,
        span_id: 0,
        name: "Goodbye, World!".try_into().unwrap(),
        start_time: SystemTime::now(),
        end_time: SystemTime::now(),
    });

    match result {
        Ok(_) => log!("Enqueued span"),
        Err(e) => error!("Failed to enqueue span: {:?}", e),
    }
}
