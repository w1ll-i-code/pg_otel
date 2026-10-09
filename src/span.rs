use std::{
    collections::BTreeSet,
    time::{Duration, SystemTime},
};

use opentelemetry::{
    InstrumentationScope, KeyValue, SpanId, TraceFlags, TraceId,
    propagation::{Extractor, TextMapPropagator},
    trace::{SpanContext, SpanKind, Status, TraceContextExt, TraceState},
};
use opentelemetry_sdk::{propagation::TraceContextPropagator, trace::SpanData};
use pgrx::pg_sys::{self, CmdType, NodeInstrumentation, PlanState, QueryDesc};

use crate::{
    config::QueryTextMode,
    postgres::{instr_ticks_to_ns, plan_table_name},
    sanitize,
};

/// Flags of every exported span. Spans are only collected when the trace is
/// sampled (a remote parent with the sampled flag cleared suppresses
/// collection entirely), so everything that reaches the exporter is sampled.
const EXPORTED_TRACE_FLAGS: TraceFlags = TraceFlags::SAMPLED;

/// Where the root span of a statement attaches to a trace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParentContext {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    /// `true` when the parent was received from outside (traceparent), `false`
    /// when this statement starts a new trace.
    pub is_remote: bool,
}

impl ParentContext {
    /// A statement without an incoming trace starts a new one: random
    /// non-zero trace id and no parent span.
    pub fn new_root() -> Self {
        Self {
            trace_id: random_trace_id(),
            span_id: SpanId::INVALID,
            is_remote: false,
        }
    }

    /// Continues the trace described by `remote`.
    pub fn from_remote(remote: &SpanContext) -> Self {
        Self {
            trace_id: remote.trace_id(),
            span_id: remote.span_id(),
            is_remote: true,
        }
    }
}

// Invariant for both generators below: `fastrand`'s thread-local generator is
// seeded lazily on first use. It must never be used in the postmaster (before
// fork), otherwise every backend would inherit the same state and produce
// identical ids. Ids are only generated inside backends (executor hook).

/// Random trace id; the all-zero id is invalid in W3C trace context.
pub fn random_trace_id() -> TraceId {
    loop {
        let id = TraceId::from(fastrand::u128(..));
        if id != TraceId::INVALID {
            return id;
        }
    }
}

/// Random span id; the all-zero id is invalid in W3C trace context.
pub fn random_span_id() -> SpanId {
    loop {
        let id = SpanId::from(fastrand::u64(..));
        if id != SpanId::INVALID {
            return id;
        }
    }
}

/// Longest query text exported, in bytes.
pub const QUERY_TEXT_MAX_LEN: usize = 4096;
/// Longest span name and relation name exported, in bytes.
const NAME_MAX_LEN: usize = 256;

/// A finished span as passed from backends to the exporter worker.
///
/// It owns its data; [`crate::codec`] turns it into the bytes that travel
/// through the shared-memory queue and back.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanRecord {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    pub parent_id: SpanId,
    pub name: String,
    pub start_time: SystemTime,
    pub end_time: SystemTime,
    pub attributes: SpanAttributes,
}

/// What children of a span need to know about it.
#[derive(Clone, Copy, Debug)]
pub struct SpanLink {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    pub end_time: SystemTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryAttributes {
    pub parent_is_remote: bool,
    /// `SELECT`, `UPDATE`, ...
    pub operation: String,
    /// `None` when `pg_otel.query_text` is `off` or the text could not be
    /// produced safely.
    pub query_text: Option<String>,
    /// Postgres' query id; `0` means it was not computed.
    pub query_id: i64,
    pub exec_total_time_ns: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PlanNodeAttributes {
    /// Debug name of the plan state node tag, e.g. `T_SeqScanState`.
    pub node_type: String,
    /// `schema.table` scanned by this node itself (not by its children).
    pub relation: Option<String>,
    pub startup_cost: f64,
    pub total_cost: f64,
    pub rows: f64,
    pub width_bytes: i32,
    pub parallel_aware: bool,
    pub parallel_safe: bool,
    pub async_capable: bool,
    pub instr_startup_time_ns: i64,
    pub instr_total_time_ns: i64,
    pub instr_rows: f64,
    pub instr_secondary_rows: f64,
    pub instr_loops: f64,
    pub instr_rows_removed_by_scan_or_join_filter: f64,
    pub instr_rows_removed_by_other_filter: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SpanAttributes {
    Query(QueryAttributes),
    PlanNode(PlanNodeAttributes),
}

impl SpanRecord {
    pub fn link(&self) -> SpanLink {
        SpanLink {
            trace_id: self.trace_id,
            span_id: self.span_id,
            end_time: self.end_time,
        }
    }

    /// The relation scanned by this plan node itself, if any.
    pub fn relation(&self) -> Option<&str> {
        match &self.attributes {
            SpanAttributes::PlanNode(node) => node.relation.as_deref(),
            SpanAttributes::Query(_) => None,
        }
    }

    /// Builds the span of a whole statement.
    ///
    /// The span covers `wall_start` up to the time Postgres spent in
    /// ExecutorRun/Finish (`query_instr.total`). `span_id` is chosen by the
    /// caller so plan nodes can reference it as their parent before this span
    /// exists, and `tables` are the relations scanned by the plan. Calls into
    /// Postgres to sanitize the query text, so it must not run while the span
    /// queue lock is held.
    pub fn from_query(
        query_desc: *const QueryDesc,
        wall_start: SystemTime,
        parent: &ParentContext,
        query_text_mode: QueryTextMode,
        span_id: SpanId,
        tables: &BTreeSet<String>,
    ) -> Option<Self> {
        let query_desc = unsafe { query_desc.as_ref()? };
        let instrument = unsafe { query_desc.query_instr.as_ref()? };

        let operation = query_name(query_desc.operation);
        let name = format!(
            "{operation} {}",
            tables
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        );

        let total_ns = instr_ticks_to_ns(instrument.total.ticks).max(0);
        let end_time = wall_start + Duration::from_nanos(total_ns as u64);
        let (query_text, query_id) = exported_query_text_and_id(query_desc, query_text_mode);

        Some(SpanRecord {
            trace_id: parent.trace_id,
            span_id,
            parent_id: parent.span_id,
            name: truncated(&name, NAME_MAX_LEN),
            start_time: wall_start,
            end_time,
            attributes: SpanAttributes::Query(QueryAttributes {
                parent_is_remote: parent.is_remote,
                operation: operation.to_owned(),
                query_text,
                query_id,
                exec_total_time_ns: total_ns,
            }),
        })
    }

    /// Builds the span of one plan node.
    ///
    /// Postgres keeps no per-node wall-clock start (only accumulated durations),
    /// so every node starts at the query start (`wall_start`) and ends after its
    /// accumulated run time, capped at the end of its parent.
    pub fn from_plan(
        plan_node: *mut PlanState,
        wall_start: SystemTime,
        parent: &SpanLink,
    ) -> Option<Self> {
        let plan_node = unsafe { plan_node.as_ref() }?;
        let instrument = unsafe { finished_node_instrumentation(plan_node.instrument) }?;
        let plan = unsafe { plan_node.plan.as_ref() }?;

        let total_ns = instr_ticks_to_ns(instrument.instr.total.ticks).max(0);
        let startup_ns = instr_ticks_to_ns(instrument.startup.ticks).max(0);
        let end_time = (wall_start + Duration::from_nanos(total_ns as u64)).min(parent.end_time);

        let relation = plan_table_name(plan_node).map(|table| truncated(&table, NAME_MAX_LEN));
        let table_suffix = relation
            .as_deref()
            .map(|table| format!(" [{table}]"))
            .unwrap_or_default();
        let node_type = format!("{:?}", plan_node.type_);
        let name = format!("postgresql.operation.{node_type}{table_suffix}");

        Some(SpanRecord {
            trace_id: parent.trace_id,
            span_id: random_span_id(),
            parent_id: parent.span_id,
            name: truncated(&name, NAME_MAX_LEN),
            start_time: wall_start,
            end_time,
            attributes: SpanAttributes::PlanNode(PlanNodeAttributes {
                node_type,
                relation,
                startup_cost: plan.startup_cost,
                total_cost: plan.total_cost,
                rows: plan.plan_rows,
                width_bytes: plan.plan_width,
                parallel_aware: plan.parallel_aware,
                parallel_safe: plan.parallel_safe,
                async_capable: plan.async_capable,
                instr_startup_time_ns: startup_ns,
                instr_total_time_ns: total_ns,
                instr_rows: instrument.ntuples,
                instr_secondary_rows: instrument.ntuples2,
                instr_loops: instrument.nloops,
                instr_rows_removed_by_scan_or_join_filter: instrument.nfiltered1,
                instr_rows_removed_by_other_filter: instrument.nfiltered2,
            }),
        })
    }
}

/// `s` cut to at most `max_bytes` bytes on a character boundary.
fn truncated(s: &str, max_bytes: usize) -> String {
    sanitize::truncate_utf8(s, max_bytes).to_owned()
}

/// Folds the node's last execution cycle into its totals (as `ExplainNode`
/// does) and returns the instrumentation, or `None` if the node has none.
///
/// `InstrEndLoop` is a no-op for a node that is not `running` (never ran, or
/// already folded in), so repeated calls are harmless. It raises an ERROR for a
/// node that is `running` while its timer is still started (execution was
/// interrupted mid-node); such a node is skipped instead of failing collection.
///
/// # Safety
///
/// `instrument` must be null or point to valid node instrumentation that no one
/// else is accessing.
unsafe fn finished_node_instrumentation<'a>(
    instrument: *mut NodeInstrumentation,
) -> Option<&'a NodeInstrumentation> {
    // SAFETY: null check by `as_mut`; validity per the function contract.
    let instrument = unsafe { instrument.as_mut()? };
    if instrument.running {
        if instrument.instr.starttime.ticks != 0 {
            return None;
        }
        // SAFETY: valid, exclusively accessed instrumentation (see above).
        unsafe { pg_sys::InstrEndLoop(instrument) };
    }
    Some(instrument)
}

/// Query text to export and the query id, according to `mode`.
fn exported_query_text_and_id(
    query_desc: &QueryDesc,
    mode: QueryTextMode,
) -> (Option<String>, i64) {
    // SAFETY: `plannedstmt` is null or valid for the duration of the hook.
    let (stmt_location, stmt_len, query_id) = match unsafe { query_desc.plannedstmt.as_ref() } {
        Some(stmt) => (stmt.stmt_location, stmt.stmt_len, stmt.queryId),
        None => (-1, 0, 0),
    };
    let text = normalize_for(mode).and_then(|normalize| {
        // SAFETY: `sourceText` is null or a NUL-terminated string that outlives
        // the hook. `sanitize` truncates to `QUERY_TEXT_MAX_LEN`.
        unsafe {
            sanitize::sanitize(
                query_desc.sourceText,
                stmt_location,
                stmt_len,
                normalize,
                QUERY_TEXT_MAX_LEN,
            )
        }
    });
    (text, query_id)
}

/// Whether the query text must be normalized; `None` means "do not export".
fn normalize_for(mode: QueryTextMode) -> Option<bool> {
    match mode {
        QueryTextMode::Off => None,
        QueryTextMode::Normalized => Some(true),
        QueryTextMode::Raw => Some(false),
    }
}

/// Seconds, for the `*_time_seconds` attributes.
fn ns_to_seconds(ns: i64) -> f64 {
    ns as f64 / 1e9
}

/// Microseconds, for `span.duration.us`.
fn ns_to_micros(ns: i64) -> f64 {
    ns as f64 / 1e3
}

fn query_name(command: CmdType::Type) -> &'static str {
    match command {
        CmdType::CMD_SELECT => "SELECT",
        CmdType::CMD_UPDATE => "UPDATE",
        CmdType::CMD_INSERT => "INSERT",
        CmdType::CMD_DELETE => "DELETE",
        CmdType::CMD_MERGE => "MERGE",
        CmdType::CMD_UTILITY => "UTILITY",
        CmdType::CMD_NOTHING => "NOTHING",
        _ => "UNKNOWN",
    }
}

impl From<SpanRecord> for SpanData {
    fn from(span: SpanRecord) -> Self {
        let span_context = SpanContext::new(
            span.trace_id,
            span.span_id,
            EXPORTED_TRACE_FLAGS,
            false,
            TraceState::default(),
        );

        let instrumentation_scope = InstrumentationScope::builder("pg_otel")
            .with_version(env!("CARGO_PKG_VERSION"))
            .build();

        match span.attributes {
            SpanAttributes::Query(attr) => {
                let mut attributes = vec![
                    KeyValue::new("db.operation.name", attr.operation),
                    KeyValue::new("db.system.name", "postgresql"),
                    KeyValue::new(
                        "postgresql.execution.total_time_seconds",
                        ns_to_seconds(attr.exec_total_time_ns),
                    ),
                    KeyValue::new("span.duration.us", ns_to_micros(attr.exec_total_time_ns)),
                    KeyValue::new("span.type", "db"),
                    KeyValue::new("span.subtype", "postgresql"),
                ];
                if let Some(query_text) = attr.query_text {
                    attributes.push(KeyValue::new("db.query.text", query_text));
                }
                if attr.query_id != 0 {
                    attributes.push(KeyValue::new("db.query.id", attr.query_id));
                }

                SpanData {
                    span_context,
                    parent_span_id: span.parent_id,
                    parent_span_is_remote: attr.parent_is_remote,
                    span_kind: SpanKind::Server,
                    name: span.name.into(),
                    start_time: span.start_time,
                    end_time: span.end_time,
                    attributes,
                    dropped_attributes_count: 0,
                    events: Default::default(),
                    links: Default::default(),
                    status: Status::Ok,
                    instrumentation_scope,
                }
            }
            SpanAttributes::PlanNode(attr) => {
                let mut attributes = vec![
                    KeyValue::new("db.system.name", "postgresql"),
                    KeyValue::new("postgresql.plan.node_type", attr.node_type),
                ];
                if let Some(relation) = attr.relation {
                    attributes.push(KeyValue::new("postgresql.plan.relation", relation));
                }
                attributes.extend([
                    KeyValue::new("postgresql.plan.startup_cost", attr.startup_cost),
                    KeyValue::new("postgresql.plan.total_cost", attr.total_cost),
                    KeyValue::new("postgresql.plan.rows", attr.rows),
                    KeyValue::new("postgresql.plan.width_bytes", attr.width_bytes as i64),
                    KeyValue::new("postgresql.plan.parallel_aware", attr.parallel_aware),
                    KeyValue::new("postgresql.plan.parallel_safe", attr.parallel_safe),
                    KeyValue::new("postgresql.plan.async_capable", attr.async_capable),
                    KeyValue::new(
                        "postgresql.instrumentation.startup_time_seconds",
                        ns_to_seconds(attr.instr_startup_time_ns),
                    ),
                    KeyValue::new(
                        "postgresql.instrumentation.total_time_seconds",
                        ns_to_seconds(attr.instr_total_time_ns),
                    ),
                    KeyValue::new("postgresql.instrumentation.rows", attr.instr_rows),
                    KeyValue::new(
                        "postgresql.instrumentation.secondary_rows",
                        attr.instr_secondary_rows,
                    ),
                    KeyValue::new("postgresql.instrumentation.loops", attr.instr_loops),
                    KeyValue::new(
                        "postgresql.instrumentation.rows_removed_by_scan_or_join_filter",
                        attr.instr_rows_removed_by_scan_or_join_filter,
                    ),
                    KeyValue::new(
                        "postgresql.instrumentation.rows_removed_by_other_filter",
                        attr.instr_rows_removed_by_other_filter,
                    ),
                    KeyValue::new("span.duration.us", ns_to_micros(attr.instr_total_time_ns)),
                    KeyValue::new("span.type", "db"),
                    KeyValue::new("span.subtype", "internal"),
                ]);

                SpanData {
                    span_context,
                    parent_span_id: span.parent_id,
                    parent_span_is_remote: false,
                    span_kind: SpanKind::Internal,
                    name: span.name.into(),
                    start_time: span.start_time,
                    end_time: span.end_time,
                    attributes,
                    dropped_attributes_count: 0,
                    events: Default::default(),
                    links: Default::default(),
                    status: Status::Ok,
                    instrumentation_scope,
                }
            }
        }
    }
}

struct TraceParentExtractor<'a> {
    parent: &'a str,
}

impl<'a> TraceParentExtractor<'a> {
    pub fn new(parent: &'a str) -> Self {
        Self { parent }
    }
}

impl Extractor for TraceParentExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        if key.eq_ignore_ascii_case("traceparent") {
            Some(self.parent)
        } else {
            None
        }
    }

    fn keys(&self) -> Vec<&str> {
        vec!["traceparent"]
    }
}

/// Parses a W3C `traceparent` header value.
///
/// Returns `None` unless the value is a valid trace context (well-formed, with
/// non-zero trace and span ids), so callers can fall back to another source.
pub fn parse_traceparent(s: &str) -> Option<SpanContext> {
    // The propagator accepts hex fields of any length, so check the shape first.
    if !has_traceparent_shape(s.trim()) {
        return None;
    }
    let propagator = TraceContextPropagator::default();
    let extractor = TraceParentExtractor::new(s);
    let context = propagator.extract(&extractor).span().span_context().clone();
    context.is_valid().then_some(context)
}

/// `version-traceid-spanid-flags` with fixed-width lowercase hex fields. Version
/// `00` has no further fields; `ff` is forbidden; future versions may append
/// fields after the four known ones.
fn has_traceparent_shape(s: &str) -> bool {
    fn is_hex(field: &str, len: usize) -> bool {
        field.len() == len
            && field
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    }

    let parts: Vec<&str> = s.split('-').collect();
    let [version, trace_id, span_id, flags, ..] = parts[..] else {
        return false;
    };
    is_hex(version, 2)
        && version != "ff"
        && (version != "00" || parts.len() == 4)
        && is_hex(trace_id, 32)
        && is_hex(span_id, 16)
        && is_hex(flags, 2)
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn parses_valid_sampled_traceparent() {
        let context = parse_traceparent(VALID).expect("valid traceparent");
        assert!(context.is_sampled());
        assert_eq!(
            context.trace_id(),
            TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap()
        );
        assert_eq!(
            context.span_id(),
            SpanId::from_hex("00f067aa0ba902b7").unwrap()
        );
    }

    #[test]
    fn accepts_surrounding_whitespace_and_future_versions() {
        assert!(parse_traceparent(&format!("  {VALID}\n")).is_some());
        assert!(
            parse_traceparent("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-more")
                .is_some()
        );
    }

    #[test]
    fn parses_unsampled_flag() {
        let context =
            parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00").unwrap();
        assert!(!context.is_sampled());
    }

    #[test]
    fn rejects_invalid_traceparents() {
        for invalid in [
            "",
            "garbage",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
            "00-4bf92f3577b34da6a3ce929d0e0e47-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba9-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e473é-00f067aa0ba902b7-01",
        ] {
            assert!(parse_traceparent(invalid).is_none(), "accepted {invalid:?}");
        }
    }

    #[test]
    fn duration_units_are_seconds_and_microseconds() {
        assert_eq!(ns_to_seconds(1_500_000_000), 1.5);
        assert_eq!(ns_to_micros(1_500_000), 1_500.0);
        assert_eq!(ns_to_micros(0), 0.0);
    }

    #[test]
    fn query_text_modes_map_to_normalization() {
        assert_eq!(normalize_for(QueryTextMode::Off), None);
        assert_eq!(normalize_for(QueryTextMode::Normalized), Some(true));
        assert_eq!(normalize_for(QueryTextMode::Raw), Some(false));
    }

    #[test]
    fn root_parent_is_new_local_trace() {
        let root = ParentContext::new_root();
        assert_ne!(root.trace_id, TraceId::INVALID);
        assert_eq!(root.span_id, SpanId::INVALID);
        assert!(!root.is_remote);
    }

    #[test]
    fn remote_parent_keeps_ids_and_is_remote() {
        let remote = parse_traceparent(VALID).unwrap();
        let parent = ParentContext::from_remote(&remote);
        assert_eq!(parent.trace_id, remote.trace_id());
        assert_eq!(parent.span_id, remote.span_id());
        assert!(parent.is_remote);
    }

    #[test]
    fn random_ids_are_never_zero() {
        for _ in 0..10_000 {
            assert_ne!(random_span_id(), SpanId::INVALID);
            assert_ne!(random_trace_id(), TraceId::INVALID);
        }
    }
}
