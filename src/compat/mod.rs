//! Differences between the supported Postgres versions (18 and 19).
//!
//! Everything version specific lives behind the small API of this module, so the
//! rest of the crate has no `cfg` attributes for it:
//!
//! * [`request_instrumentation_before_start`] / [`request_instrumentation_after_start`]
//!   ask the executor for the timers and row counts that spans are built from.
//! * [`query_total_ns`] is the time the statement spent executing.
//! * [`node_instrument`] reads the finished instrumentation of a plan node.
//! * [`subplan_display_name`] names a sub-plan the way `EXPLAIN` does.
//! * [`log_min_messages_slot`] finds the `log_min_messages` setting of this
//!   backend.
//!
//! Exactly one of the cargo features `pg18` and `pg19` must be enabled; the
//! default is `pg19`.
//!
//! # Where the versions differ
//!
//! | | PG18 | PG19 |
//! | --- | --- | --- |
//! | `instr_time.ticks` | nanoseconds | clock ticks (TSC), converted with `pg_ticks_to_ns` |
//! | per-node instrumentation | `Instrumentation`, times as `double` seconds | `NodeInstrumentation`, times as `instr_time` |
//! | statement instrumentation | `QueryDesc.totaltime`, allocated by the hook after `ExecutorStart` | `QueryDesc.query_instr`, requested with `query_instr_options` before |
//! | sub-plan names | `SubPlan.plan_name` is complete (`InitPlan 1`) | `plan_name` is bare; `isInitPlan` and `subLinkType` say what it is |
//! | `log_min_messages` | one `int` | an array with one level per backend type |

// Exactly one of `pg18` and `pg19` must be enabled. That is enforced by
// `pgrx-pg-sys`, which refuses to build with none or with several Postgres
// version features, before this crate is compiled.
#[cfg(feature = "pg18")]
mod pg18;
#[cfg(feature = "pg18")]
pub use pg18::*;

#[cfg(feature = "pg19")]
mod pg19;
#[cfg(feature = "pg19")]
pub use pg19::*;

/// The accumulated measurements of one plan node, in nanoseconds and counts.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct NodeStats {
    pub startup_ns: i64,
    pub total_ns: i64,
    pub rows: f64,
    pub secondary_rows: f64,
    pub loops: f64,
    pub filtered_by_scan_or_join: f64,
    pub filtered_by_other: f64,
}

/// What could be read from a plan node's instrumentation.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum NodeInstrument {
    /// The last execution cycle was folded into the totals.
    Done(NodeStats),
    /// There is no instrumentation, or the node was interrupted mid-run.
    Incomplete,
}

/// Whether a node's instrumentation is in the state in which `InstrEndLoop`
/// raises an ERROR: its cycle is `running` (a tuple was produced) while its timer
/// is still started (`starttime` is not zero). That means execution was
/// interrupted inside the node.
///
/// `starttime_ticks` is the raw `starttime.ticks`; zero means "not started".
pub(crate) fn is_mid_timer(running: bool, starttime_ticks: i64) -> bool {
    running && starttime_ticks != 0
}

/// The per-node instrumentation that spans are built from: row counts and
/// wall-clock timers.
pub(crate) fn instrument_options() -> i32 {
    (pgrx::pg_sys::InstrumentOption::INSTRUMENT_ROWS
        | pgrx::pg_sys::InstrumentOption::INSTRUMENT_TIMER) as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_running_node_with_a_started_timer_is_mid_timer() {
        assert!(is_mid_timer(true, 5));
        assert!(!is_mid_timer(true, 0));
        assert!(!is_mid_timer(false, 5));
        assert!(!is_mid_timer(false, 0));
    }
}
