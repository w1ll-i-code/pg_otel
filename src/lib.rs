use std::time::Duration;

use pgrx::{
    bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags},
    prelude::*,
};

use crate::{
    config::ExporterConfig,
    postgres::{collect_spans, complete_instrumentation_request, request_instrumentation},
    worker::background_worker_run,
};

mod codec;
mod compat;
mod config;
mod postgres;
mod queue;
mod sanitize;
mod shared;
mod span;
mod worker;

::pgrx::pg_module_magic!(name, version);

/// Name of the exporter background worker as shown in `pg_stat_activity`.
const WORKER_NAME: &str = "pg_otel exporter";

/// Postgres restarts the worker this long after it crashed or exited.
const WORKER_RESTART_TIME: Duration = Duration::from_secs(10);

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

    shared::install_hooks();

    BackgroundWorkerBuilder::new(WORKER_NAME)
        .set_function("background_worker_main")
        .set_library("pg_otel")
        .set_restart_time(Some(WORKER_RESTART_TIME))
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
    unsafe { pg_sys::MarkGUCPrefixReserved(c"pg_otel".as_ptr()) };
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_main(_arg: pg_sys::Datum) {
    // These are the signals we want to receive. Without the SIGTERM handler we
    // would never be able to exit via an external notification. Backends wake
    // the worker through its latch, not with signals.
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // Publish this process so backends can set its latch; the registration is
    // removed again when the process exits, whatever the reason.
    shared::register_worker();

    background_worker_run();

    log!("{} is shutting down", BackgroundWorker::get_name());

    // Exit with a non-zero status: the postmaster then restarts the worker
    // after `WORKER_RESTART_TIME` (a zero status would mean "do not restart",
    // so `pg_terminate_backend()` on the worker would silently end exporting
    // until the next server start). During a server shutdown the postmaster
    // does not restart workers; it only logs the exit code. `proc_exit` runs
    // the exit callbacks that unregister the worker.
    // SAFETY: called from the worker's main function at the end of its life.
    unsafe { pg_sys::proc_exit(1) };
}

/// Number of spans dropped since server start because the span queue was full.
#[pg_extern]
fn pg_otel_dropped_spans() -> i64 {
    i64::try_from(shared::dropped_spans()).unwrap_or(i64::MAX)
}

#[pg_guard]
unsafe extern "C-unwind" fn my_executor_start_hook(
    query_desc: *mut pg_sys::QueryDesc,
    eflags: i32,
) {
    // Does nothing when tracing is disabled, in parallel workers, or for
    // EXPLAIN without ANALYZE.
    request_instrumentation(query_desc, eflags);

    // SAFETY: I am trusting the docs on this one.
    unsafe {
        if let Some(prev) = PREV_EXECUTOR_START {
            prev(query_desc, eflags);
        } else {
            pg_sys::standard_ExecutorStart(query_desc, eflags);
        }
    }

    // PG18 can only allocate the statement timer once the executor state
    // exists; a no-op on PG19.
    complete_instrumentation_request(query_desc, eflags);
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
        // pg_stat_statements comes *after* pg_otel on purpose: it needs
        // statement-level buffer and WAL usage that pg_otel's own
        // instrumentation request must not take away (see the test
        // `pg_stat_statements_still_sees_buffer_usage`).
        vec!["shared_preload_libraries = 'pg_otel,pg_stat_statements'"]
    }
}
