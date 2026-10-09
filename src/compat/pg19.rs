//! PostgreSQL 19.

use pgrx::pg_sys::{self, NodeInstrumentation, PlanState, QueryDesc};

use std::ffi::c_int;

use super::{NodeInstrument, NodeStats, instrument_options, is_mid_timer};

/// Fixed-point shift used by Postgres to convert instrumentation ticks to
/// nanoseconds (`TICKS_TO_NS_SHIFT` in `portability/instr_time.h`, a macro and
/// therefore not part of the bindings).
const TICKS_TO_NS_SHIFT: u32 = 14;

/// Converts `instr_time` ticks to nanoseconds with the given scale factors.
///
/// This is a port of the static inline `pg_ticks_to_ns` of
/// `portability/instr_time.h`. Since PG19, `instr_time.ticks` counts raw clock
/// ticks (for example TSC ticks on x86-64), not nanoseconds. A
/// `ticks_per_ns_scaled` of zero means the clock already counts nanoseconds.
/// Large tick counts are scaled in two parts exactly as Postgres does so the
/// multiplication cannot overflow; the arithmetic saturates instead of
/// wrapping should the inputs ever be inconsistent.
fn ticks_to_ns(ticks: i64, ticks_per_ns_scaled: u64, max_ticks_no_overflow: u64) -> i64 {
    if ticks_per_ns_scaled == 0 {
        return ticks;
    }
    let scale = i64::try_from(ticks_per_ns_scaled).unwrap_or(i64::MAX);
    let mut ticks = ticks;
    let mut ns = 0_i64;
    if ticks > i64::try_from(max_ticks_no_overflow).unwrap_or(i64::MAX) {
        let count = ticks >> TICKS_TO_NS_SHIFT;
        ns = count.saturating_mul(scale);
        ticks -= count << TICKS_TO_NS_SHIFT;
    }
    ns.saturating_add(ticks.saturating_mul(scale) >> TICKS_TO_NS_SHIFT)
}

/// Converts `instr_time.ticks` read from Postgres instrumentation to
/// nanoseconds using this backend's timing configuration.
fn instr_ticks_to_ns(ticks: i64) -> i64 {
    // SAFETY: plain reads of process-wide variables that are initialised at
    // backend start (`pg_initialize_timing`).
    let (scale, max) = unsafe { (pg_sys::ticks_per_ns_scaled, pg_sys::max_ticks_no_overflow) };
    ticks_to_ns(ticks, scale, max)
}

/// Requests statement and per-node instrumentation. Must run before
/// `ExecutorStart` creates the plan state.
///
/// The options are OR-ed in: bits requested by other extensions (for example
/// `INSTRUMENT_ALL` by `pg_stat_statements`, which needs buffer and WAL usage)
/// are never cleared, so the result does not depend on the order of
/// `shared_preload_libraries`.
///
/// # Safety
///
/// `query_desc` must point to a valid `QueryDesc`.
pub unsafe fn request_instrumentation_before_start(query_desc: *mut QueryDesc) {
    // SAFETY: valid per the function contract.
    unsafe {
        (*query_desc).query_instr_options |= instrument_options();
        (*query_desc).instrument_options |= instrument_options();
    }
}

/// Nothing to do after `ExecutorStart` on PG19 (see the PG18 version).
///
/// # Safety
///
/// `query_desc` must point to a valid `QueryDesc`.
pub unsafe fn request_instrumentation_after_start(_query_desc: *mut QueryDesc) {}

/// Nanoseconds the statement spent in ExecutorRun and ExecutorFinish, or
/// `None` if no statement instrumentation was requested.
///
/// # Safety
///
/// `query_desc` must point to a valid `QueryDesc` in `ExecutorEnd`.
pub unsafe fn query_total_ns(query_desc: &QueryDesc) -> Option<i64> {
    // SAFETY: `query_instr` is null or valid.
    let instrument = unsafe { query_desc.query_instr.as_ref()? };
    Some(instr_ticks_to_ns(instrument.total.ticks).max(0))
}

/// Folds the node's last execution cycle into its totals (as `ExplainNode`
/// does) and returns its measurements.
///
/// `InstrEndLoop` is a no-op for a node that is not `running` (never ran, or
/// already folded in), so repeated calls are harmless. It raises an ERROR for a
/// node that is `running` while its timer is still started (execution was
/// interrupted mid-node); that node is reported as
/// [`NodeInstrument::Incomplete`] instead of failing the whole collection.
///
/// # Safety
///
/// `plan_node` must be a valid plan state whose instrumentation no one else is
/// accessing.
pub unsafe fn node_instrument(plan_node: &PlanState) -> NodeInstrument {
    let instrument: *mut NodeInstrumentation = plan_node.instrument;
    // SAFETY: null or valid per the function contract.
    let Some(instrument) = (unsafe { instrument.as_mut() }) else {
        return NodeInstrument::Incomplete;
    };
    if is_mid_timer(instrument.running, instrument.instr.starttime.ticks) {
        return NodeInstrument::Incomplete;
    }
    // SAFETY: valid, exclusively accessed instrumentation (see above); a no-op
    // when the node is not running.
    unsafe { pg_sys::InstrEndLoop(instrument) };
    NodeInstrument::Done(NodeStats {
        startup_ns: instr_ticks_to_ns(instrument.startup.ticks).max(0),
        total_ns: instr_ticks_to_ns(instrument.instr.total.ticks).max(0),
        rows: instrument.ntuples,
        secondary_rows: instrument.ntuples2,
        loops: instrument.nloops,
        filtered_by_scan_or_join: instrument.nfiltered1,
        filtered_by_other: instrument.nfiltered2,
    })
}

/// This backend's `log_min_messages` setting.
///
/// PG19 keeps one level per backend type: the C variable is an array declared
/// as `extern int log_min_messages[];` in `utils/guc.h`, which bindgen exposes as
/// a zero-length array. Indexing it directly would be out of bounds for the Rust
/// type, so the slot is reached through raw pointer arithmetic on the array's
/// address. The bounds check against `B_LOGGER` (the last backend type) keeps the
/// offset inside the real array.
///
/// # Safety
///
/// Must be called in a Postgres backend (`MyBackendType` is set).
pub unsafe fn log_min_messages_slot() -> Option<*mut c_int> {
    // SAFETY: plain read of a backend-local variable.
    let backend_type = unsafe { pg_sys::MyBackendType } as usize;
    (backend_type <= pg_sys::BackendType::B_LOGGER as usize).then(|| {
        // SAFETY: `backend_type` is within the array (checked above).
        unsafe {
            (&raw mut pg_sys::log_min_messages)
                .cast::<c_int>()
                .add(backend_type)
        }
    })
}

/// The name `EXPLAIN` gives a sub-plan: `CTE x`, `InitPlan 1` or `SubPlan 2`.
/// In PG19 `plan_name` is the bare name chosen by the planner.
pub fn subplan_display_name(subplan: &pg_sys::SubPlan) -> String {
    let plan_name = crate::postgres::pg_str(subplan.plan_name);
    cook_subplan_name(
        subplan.subLinkType == pg_sys::SubLinkType::CTE_SUBLINK,
        subplan.isInitPlan,
        plan_name.as_deref().unwrap_or_default(),
    )
}

fn cook_subplan_name(is_cte: bool, is_init_plan: bool, plan_name: &str) -> String {
    let kind = if is_cte {
        "CTE"
    } else if is_init_plan {
        "InitPlan"
    } else {
        "SubPlan"
    };
    format!("{kind} {plan_name}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Exact reference for the conversion: `ticks * scale / 2^14`, rounded down.
    fn reference_ticks_to_ns(ticks: i64, scale: u64) -> i64 {
        ((i128::from(ticks) * i128::from(scale)) >> TICKS_TO_NS_SHIFT) as i64
    }

    #[test]
    fn ticks_are_nanoseconds_when_scale_is_zero() {
        assert_eq!(ticks_to_ns(123_456, 0, 0), 123_456);
        assert_eq!(ticks_to_ns(i64::MAX, 0, 0), i64::MAX);
    }

    #[test]
    fn converts_tsc_ticks_with_fixed_point_scale() {
        // 3 GHz TSC: scale = (1e6 << 14) / 3_000_000 kHz = 5461 (1/3 ns per tick).
        let scale = (1_000_000_u64 << TICKS_TO_NS_SHIFT) / 3_000_000;
        let max = (i64::MAX as u64) / scale;
        assert_eq!(scale, 5461);
        assert_eq!(ticks_to_ns(0, scale, max), 0);
        assert_eq!(ticks_to_ns(16_384, scale, max), 5_461);
        // One second of ticks is one second up to the fixed-point rounding error.
        let one_second = ticks_to_ns(3_000_000_000, scale, max);
        assert!(
            (999_000_000..=1_000_000_000).contains(&one_second),
            "{one_second}"
        );
    }

    #[test]
    fn matches_exact_arithmetic_including_the_overflow_branch() {
        for &(scale, max_ticks) in &[
            (5461_u64, i64::MAX as u64 / 5461),
            (16_384, i64::MAX as u64 / 16_384),
            (1, i64::MAX as u64),
        ] {
            let mut samples = vec![
                0,
                1,
                16_383,
                16_384,
                16_385,
                max_ticks as i64 - 1,
                max_ticks as i64,
                (max_ticks as i64).saturating_add(1),
                i64::MAX / 2,
            ];
            let mut rng = fastrand::Rng::with_seed(7);
            samples.extend((0..2_000).map(|_| rng.i64(0..=max_ticks as i64)));
            samples.extend((0..2_000).map(|_| rng.i64(0..=i64::MAX)));
            for ticks in samples {
                let expected = reference_ticks_to_ns(ticks, scale);
                assert_eq!(
                    ticks_to_ns(ticks, scale, max_ticks),
                    expected,
                    "ticks={ticks} scale={scale}"
                );
            }
        }
    }

    #[test]
    fn conversion_saturates_instead_of_overflowing() {
        // Inconsistent inputs (max too large for the scale) must not panic.
        assert!(ticks_to_ns(i64::MAX, 1 << 40, u64::MAX) > 0);
        assert!(ticks_to_ns(i64::MAX, u64::MAX, 0) > 0);
    }

    #[test]
    fn subplan_names_follow_explain() {
        assert_eq!(cook_subplan_name(false, true, "1"), "InitPlan 1");
        assert_eq!(cook_subplan_name(false, false, "2"), "SubPlan 2");
        assert_eq!(cook_subplan_name(true, true, "c"), "CTE c");
        assert_eq!(cook_subplan_name(true, false, "c"), "CTE c");
    }
}
