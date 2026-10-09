//! PostgreSQL 18.

use pgrx::pg_sys::{self, Instrumentation, PlanState, QueryDesc};

use std::ffi::c_int;

use super::{NodeInstrument, NodeStats, instrument_options, is_mid_timer};

/// Converts a time in seconds (how PG18 stores accumulated durations) to
/// nanoseconds, rounding to the nearest. Negative and NaN values become 0.
fn seconds_to_ns(seconds: f64) -> i64 {
    if seconds.is_nan() || seconds <= 0.0 {
        return 0;
    }
    // `as` saturates at i64::MAX for huge values.
    (seconds * 1e9).round() as i64
}

/// Requests per-node instrumentation. Must run before `ExecutorStart` creates
/// the plan state. Statement-level timing is requested separately, after
/// `ExecutorStart` (see [`request_instrumentation_after_start`]).
///
/// # Safety
///
/// `query_desc` must point to a valid `QueryDesc`.
pub unsafe fn request_instrumentation_before_start(query_desc: *mut QueryDesc) {
    // SAFETY: valid per the function contract.
    unsafe { (*query_desc).instrument_options |= instrument_options() };
}

/// Allocates the statement-level instrumentation (`QueryDesc.totaltime`), the
/// way `auto_explain` and `pg_stat_statements` do: after `ExecutorStart`, in
/// the per-query memory context, unless some other hook already did. The
/// executor then times ExecutorRun and ExecutorFinish with it.
///
/// It is allocated with `INSTRUMENT_ALL`, exactly like those extensions do:
/// there is only one `totaltime` per statement, and whoever allocates it first
/// decides what it measures. `pg_stat_statements` reports buffer and WAL usage
/// from it; a timer with only rows and timing would silently zero those numbers
/// when pg_otel is listed before it in `shared_preload_libraries`. (Per-node
/// instrumentation, which is not shared this way, stays rows and timer only.)
///
/// # Safety
///
/// `query_desc` must point to a valid `QueryDesc` whose `ExecutorStart` has
/// completed.
pub unsafe fn request_instrumentation_after_start(query_desc: *mut QueryDesc) {
    // SAFETY: valid per the function contract; `estate` exists after start.
    unsafe {
        let query_desc = &mut *query_desc;
        if !query_desc.totaltime.is_null() || query_desc.estate.is_null() {
            return;
        }
        let previous_context = pg_sys::CurrentMemoryContext;
        pg_sys::CurrentMemoryContext = (*query_desc.estate).es_query_cxt;
        query_desc.totaltime =
            pg_sys::InstrAlloc(1, pg_sys::InstrumentOption::INSTRUMENT_ALL as i32, false);
        pg_sys::CurrentMemoryContext = previous_context;
    }
}

/// Nanoseconds the statement spent in ExecutorRun and ExecutorFinish, or
/// `None` if there is no usable statement timer.
///
/// `totaltime` accumulates into `total` only when `InstrEndLoop` runs; it is
/// called here like `pg_stat_statements` does in its `ExecutorEnd`, and it is
/// harmless if several hooks do it.
///
/// # Safety
///
/// `query_desc` must point to a valid `QueryDesc` in `ExecutorEnd`.
pub unsafe fn query_total_ns(query_desc: &QueryDesc) -> Option<i64> {
    // SAFETY: `totaltime` is null or valid; nothing else touches it now.
    let totaltime = unsafe { query_desc.totaltime.as_mut()? };
    if !totaltime.need_timer {
        // Allocated by another extension without a timer.
        return None;
    }
    finished(totaltime).then(|| seconds_to_ns(totaltime.total))
}

/// Folds the node's last execution cycle into its totals (as `ExplainNode`
/// does) and returns its measurements. See the PG19 version for the semantics;
/// here `InstrEndLoop` raises an ERROR for the same "still running" condition.
///
/// # Safety
///
/// `plan_node` must be a valid plan state whose instrumentation no one else is
/// accessing.
pub unsafe fn node_instrument(plan_node: &PlanState) -> NodeInstrument {
    // SAFETY: null or valid per the function contract.
    let Some(instrument) = (unsafe { plan_node.instrument.as_mut() }) else {
        return NodeInstrument::Incomplete;
    };
    if !finished(instrument) {
        return NodeInstrument::Incomplete;
    }
    NodeInstrument::Done(NodeStats {
        startup_ns: seconds_to_ns(instrument.startup),
        total_ns: seconds_to_ns(instrument.total),
        rows: instrument.ntuples,
        secondary_rows: instrument.ntuples2,
        loops: instrument.nloops,
        filtered_by_scan_or_join: instrument.nfiltered1,
        filtered_by_other: instrument.nfiltered2,
    })
}

/// Folds the last cycle into the totals with `InstrEndLoop`, unless the timer
/// is still started (which would raise an ERROR). Returns whether the totals
/// are usable.
fn finished(instrument: &mut Instrumentation) -> bool {
    if is_mid_timer(instrument.running, instrument.starttime.ticks) {
        return false;
    }
    // SAFETY: valid, exclusively accessed instrumentation; a no-op when the
    // node is not running.
    unsafe { pg_sys::InstrEndLoop(instrument) };
    true
}

/// This backend's `log_min_messages` setting: a single `int` in PG18.
///
/// # Safety
///
/// Must be called in a Postgres backend.
pub unsafe fn log_min_messages_slot() -> Option<*mut c_int> {
    Some(&raw mut pg_sys::log_min_messages)
}

/// The name `EXPLAIN` gives a sub-plan. In PG18 the planner already stores the
/// complete name (`InitPlan 1`, `SubPlan 2`, `CTE x`) in `plan_name`.
pub fn subplan_display_name(subplan: &pg_sys::SubPlan) -> String {
    crate::postgres::pg_str(subplan.plan_name)
        .map(std::borrow::Cow::into_owned)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seconds_convert_to_rounded_nanoseconds() {
        assert_eq!(seconds_to_ns(0.0), 0);
        assert_eq!(seconds_to_ns(1.0), 1_000_000_000);
        assert_eq!(seconds_to_ns(1.5e-6), 1_500);
        assert_eq!(seconds_to_ns(0.050_000_000_4), 50_000_000);
        assert_eq!(seconds_to_ns(0.050_000_000_6), 50_000_001);
    }

    #[test]
    fn invalid_durations_become_zero_and_huge_ones_saturate() {
        assert_eq!(seconds_to_ns(-1.0), 0);
        assert_eq!(seconds_to_ns(f64::NAN), 0);
        assert_eq!(seconds_to_ns(f64::INFINITY), i64::MAX);
        assert_eq!(seconds_to_ns(1e30), i64::MAX);
    }
}
