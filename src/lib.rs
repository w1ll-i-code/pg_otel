use std::{
    ffi::CStr,
    time::{Duration, SystemTime},
};

use pgrx::{
    bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags},
    pg_shmem_init,
    pg_sys::InstrumentOption::{INSTRUMENT_ROWS, INSTRUMENT_TIMER},
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

    'outer: while BackgroundWorker::wait_latch(Some(Duration::from_millis(100))) {
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
                None => break,
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
    request_instrumentation(query_desc);

    if let Some(prev) = PREV_EXECUTOR_START {
        prev(query_desc, eflags);
    } else {
        pg_sys::standard_ExecutorStart(query_desc, eflags);
    }
}

pub fn request_instrumentation(query_desc: *mut pg_sys::QueryDesc) {
    if query_desc.is_null() {
        return;
    }

    // SAFETY: We check that `query_desc` is not null before dereferencing it.
    unsafe {
        (*query_desc).query_instr_options |= (INSTRUMENT_ROWS | INSTRUMENT_TIMER) as i32;
        (*query_desc).instrument_options |= (INSTRUMENT_ROWS | INSTRUMENT_TIMER) as i32;
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn my_executor_end_hook(query_desc: *mut pg_sys::QueryDesc) {
    collect_spans(query_desc);

    if let Some(prev) = PREV_EXECUTOR_END {
        prev(query_desc);
    } else {
        pg_sys::standard_ExecutorEnd(query_desc);
    }
}

pub fn collect_spans(query_desc: *mut pg_sys::QueryDesc) {
    if query_desc.is_null() {
        return;
    }

    // Use the current time to calculate the duration of the query.
    // This should be close enough to the actual end time.
    let end_time = SystemTime::now();
    let total = unsafe { (*(*query_desc).query_instr).total.ticks };
    let wall_start = end_time - Duration::from_nanos(total as u64);

    let source_text = unsafe { (*query_desc).sourceText };
    let name = pg_str(source_text).unwrap_or_default();
    let instumentation = unsafe { (*query_desc).query_instr };
    enqueue_spans(name, wall_start, instumentation);

    let planstate = unsafe { (*query_desc).planstate };
    collect_plan_spans(planstate, wall_start);
}

pub fn collect_plan_spans(planstate: *mut pg_sys::PlanState, wall_start: SystemTime) {
    if planstate.is_null() {
        return;
    }
    let type_ = unsafe { (*planstate).type_ };
    let name = format!("{:?}", type_);
    let instumentation = unsafe { &raw const (*(*planstate).instrument).instr };
    enqueue_spans(&name, wall_start, instumentation);

    let lefttree = unsafe { (*planstate).lefttree };
    let righttree = unsafe { (*planstate).righttree };
    if !lefttree.is_null() {
        collect_plan_spans(lefttree, wall_start);
    }
    if !righttree.is_null() {
        collect_plan_spans(righttree, wall_start);
    }
}

pub fn enqueue_spans(
    name: &str,
    wall_start: SystemTime,
    instumentation: *const pg_sys::Instrumentation,
) {
    if instumentation.is_null() {
        return;
    }
    let start_ns = unsafe { (*instumentation).starttime.ticks };
    let total_time = unsafe { (*instumentation).total.ticks };
    let end_ns = start_ns + total_time;

    let span = HeaplessSpan {
        start_time: wall_start + Duration::from_nanos(start_ns as u64),
        end_time: wall_start + Duration::from_nanos(end_ns as u64),
        name: name.try_into().unwrap_or_default(),
        trace_id: 0,
        span_id: 0,
    };

    let _ = DEQUE.exclusive().enqueue(span).ok();
}

pub fn pg_str<'a>(s: *const i8) -> Option<&'a str> {
    // Check utf-8 validity
    let cstr = unsafe { CStr::from_ptr(s) };
    cstr.to_str().ok()
}
