#![allow(dead_code)]

use std::{
    time::{Duration, SystemTime},
    usize,
};

use opentelemetry::{
    Array, InstrumentationScope, KeyValue, SpanId, StringValue, TraceFlags, TraceId, Value,
    propagation::{Extractor, TextMapPropagator},
    trace::{SpanContext, SpanKind, Status, TraceContextExt, TraceState},
};
use opentelemetry_sdk::{propagation::TraceContextPropagator, trace::SpanData};
use pgrx::{
    log,
    pg_sys::{CmdType, NodeTag, PlanState, QueryDesc},
};

use crate::postgres::{collect_table_names, pg_str, plan_table_name};

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

const QUERY_TEXT_MAX_LEN: usize = 512;
const PLAN_NODE_NAME_MAX_LEN: usize = 64;
const PLAN_TABLES_MAX_LEN: usize = 12;

pub struct HeaplessSpan {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    pub parent_id: SpanId,
    pub name: heapless::String<PLAN_NODE_NAME_MAX_LEN>,
    pub start_time: SystemTime,
    pub end_time: SystemTime,
    pub attributes: HeaplessSpanAttributes,
}

pub struct QueryAttributes {
    parent_is_remote: bool,
    operation: CmdType::Type,
    query_text: heapless::String<QUERY_TEXT_MAX_LEN>,
    exec_startup_time_seconds: f64,
    exec_total_time_seconds: f64,
}

pub struct PlanNodeAttributes {
    plan_node_type: NodeTag,
    plan_tables: heapless::Vec<heapless::String<PLAN_NODE_NAME_MAX_LEN>, PLAN_TABLES_MAX_LEN>,
    plan_startup_cost: f64,
    plan_total_cost: f64,
    plan_rows: f64,
    plan_width_bytes: i32,
    plan_parallel_aware: bool,
    plan_parallel_safe: bool,
    plan_async_capable: bool,
    instr_startup_time_seconds: f64,
    instr_total_time_seconds: f64,
    instr_rows: f64,
    instr_secondary_rows: f64,
    instr_loops: f64,
    instr_rows_removed_by_scan_or_join_filter: f64,
    instr_rows_removed_by_other_filter: f64,
}

pub enum HeaplessSpanAttributes {
    Query(QueryAttributes),
    PlanNode(PlanNodeAttributes),
}

impl HeaplessSpan {
    pub fn from_query(
        query_desc: *const QueryDesc,
        wall_start: SystemTime,
        parent: &ParentContext,
    ) -> Option<Self> {
        let query_desc = unsafe { query_desc.as_ref()? };
        let plan_state = unsafe { (*query_desc).planstate.as_ref()? };
        let instrument = unsafe { query_desc.query_instr.as_ref()? };

        let operation = query_desc.operation;
        let name = {
            let query_name = query_name(operation);
            let mut tables = collect_table_names(plan_state as *const PlanState);
            tables.sort_unstable();
            String::from(query_name) + " " + &tables.join(", ")
        };

        let query_text = pg_str(query_desc.sourceText).unwrap_or_default();

        let start_time = wall_start + Duration::from_nanos(instrument.starttime.ticks as u64);
        let end_time = start_time + Duration::from_nanos(instrument.total.ticks as u64);
        let exec_startup_time_seconds = instrument.starttime.ticks as f64 / 1e9;
        let exec_total_time_seconds = instrument.total.ticks as f64 / 1e9;

        Some(HeaplessSpan {
            trace_id: parent.trace_id,
            span_id: random_span_id(),
            parent_id: parent.span_id,
            name: truncate(&name),
            start_time,
            end_time,
            attributes: HeaplessSpanAttributes::Query(QueryAttributes {
                parent_is_remote: parent.is_remote,
                operation,
                query_text: truncate(&query_text),
                exec_startup_time_seconds,
                exec_total_time_seconds,
            }),
        })
    }

    pub fn from_plan(
        plan_node: *const PlanState,
        wall_start: SystemTime,
        parent: &HeaplessSpan,
    ) -> Option<Self> {
        let plan_node = unsafe { plan_node.as_ref() }?;
        let instrument = unsafe { plan_node.instrument.as_ref() }?;
        let plan = unsafe { plan_node.plan.as_ref() }?;

        let start_time = wall_start + Duration::from_nanos(instrument.instr.starttime.ticks as u64);
        let end_time = wall_start + Duration::from_nanos(instrument.instr.total.ticks as u64);

        let plan_table_names = collect_table_names(plan_node);
        let plan_table_len = plan_table_names.len();
        let table_suffix = plan_table_name(plan_node)
            .map(|table| format!(" [{}]", table))
            .unwrap_or_default();
        let name = format!("postgresql.operation.{:?}{}", plan_node.type_, table_suffix);
        let name_end = name.floor_char_boundary(PLAN_NODE_NAME_MAX_LEN);
        let name = heapless::String::try_from(&name[..name_end]).expect("name was truncated");

        let mut plan_tables = heapless::Vec::new();
        for table_name in plan_table_names {
            if let Ok(table_name) = heapless::String::try_from(table_name.as_str()) {
                if plan_tables.push(table_name).is_err() {
                    log!("Too many plan table names: {}", plan_table_len);
                }
            } else {
                log!("Plan table name too long: {}", table_name);
            }
        }

        Some(HeaplessSpan {
            trace_id: parent.trace_id,
            span_id: random_span_id(),
            parent_id: parent.span_id,
            name,
            start_time,
            end_time,
            attributes: HeaplessSpanAttributes::PlanNode(PlanNodeAttributes {
                plan_node_type: plan_node.type_,
                plan_startup_cost: plan.startup_cost,
                plan_total_cost: plan.total_cost,
                plan_rows: plan.plan_rows,
                plan_width_bytes: plan.plan_width,
                plan_parallel_aware: plan.parallel_aware,
                plan_parallel_safe: plan.parallel_safe,
                plan_async_capable: plan.async_capable,
                plan_tables,
                instr_startup_time_seconds: instrument.startup.ticks as f64 / 1e9,
                instr_total_time_seconds: instrument.instr.total.ticks as f64 / 1e9,
                instr_rows: instrument.ntuples,
                instr_secondary_rows: instrument.ntuples2,
                instr_loops: instrument.nloops,
                instr_rows_removed_by_scan_or_join_filter: instrument.nfiltered1,
                instr_rows_removed_by_other_filter: instrument.nfiltered2,
            }),
        })
    }
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

impl From<HeaplessSpan> for SpanData {
    fn from(span: HeaplessSpan) -> Self {
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
            HeaplessSpanAttributes::Query(attr) => {
                let operation = query_name(attr.operation);
                let query_text = attr.query_text.as_str().to_owned();

                SpanData {
                    span_context,
                    parent_span_id: span.parent_id,
                    parent_span_is_remote: attr.parent_is_remote,
                    span_kind: SpanKind::Server,
                    name: span.name.as_str().to_owned().into(),
                    start_time: span.start_time,
                    end_time: span.end_time,
                    attributes: vec![
                        KeyValue::new("db.operation", operation),
                        KeyValue::new("db.statement", query_text),
                        KeyValue::new("db.system", "postgresql"),
                        KeyValue::new(
                            "postgresql.execution.startup_time_seconds",
                            attr.exec_startup_time_seconds,
                        ),
                        KeyValue::new(
                            "postgresql.execution.total_time_seconds",
                            attr.exec_total_time_seconds,
                        ),
                        KeyValue::new("span.duration.us", attr.exec_total_time_seconds / 1e6),
                        KeyValue::new("span.type", "db"),
                        KeyValue::new("span.subtype", "postgresql"),
                    ],
                    dropped_attributes_count: 0,
                    events: Default::default(),
                    links: Default::default(),
                    status: Status::Ok,
                    instrumentation_scope,
                }
            }
            HeaplessSpanAttributes::PlanNode(attr) => {
                let node_type = format!("{:?}", attr.plan_node_type);

                let plan_tables = attr
                    .plan_tables
                    .iter()
                    .map(ToString::to_string)
                    .map(StringValue::from)
                    .collect::<Vec<_>>();

                SpanData {
                    span_context,
                    parent_span_id: span.parent_id,
                    parent_span_is_remote: false,
                    span_kind: SpanKind::Internal,
                    name: span.name.as_str().to_owned().into(),
                    start_time: span.start_time,
                    end_time: span.end_time,
                    attributes: vec![
                        KeyValue::new("db.system", "postgresql"),
                        KeyValue::new("postgresql.plan.node_type", node_type),
                        KeyValue::new(
                            "postgresql.plan.tables",
                            Value::Array(Array::String(plan_tables)),
                        ),
                        KeyValue::new("postgresql.plan.startup_cost", attr.plan_startup_cost),
                        KeyValue::new("postgresql.plan.total_cost", attr.plan_total_cost),
                        KeyValue::new("postgresql.plan.rows", attr.plan_rows),
                        KeyValue::new("postgresql.plan.width_bytes", attr.plan_width_bytes as i64),
                        KeyValue::new("postgresql.plan.parallel_aware", attr.plan_parallel_aware),
                        KeyValue::new("postgresql.plan.parallel_safe", attr.plan_parallel_safe),
                        KeyValue::new("postgresql.plan.async_capable", attr.plan_async_capable),
                        KeyValue::new(
                            "postgresql.instrumentation.startup_time_seconds",
                            attr.instr_startup_time_seconds,
                        ),
                        KeyValue::new(
                            "postgresql.instrumentation.total_time_seconds",
                            attr.instr_total_time_seconds,
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
                        KeyValue::new("span.duration.us", attr.instr_total_time_seconds * 1e3),
                        KeyValue::new("span.type", "db"),
                        KeyValue::new("span.subtype", "internal"),
                    ],
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

pub fn truncate<const N: usize>(s: &str) -> heapless::String<N> {
    let s = s.trim();
    assert!(N > 3);
    if s.len() <= N {
        return heapless::String::try_from(s).expect("length was checked beforehand");
    }

    let truncate_at = s.floor_char_boundary(N - 3);
    let mut truncated =
        heapless::String::try_from(&s[..truncate_at]).expect("length was checked beforehand");
    truncated
        .push('…')
        .expect("truncation left room for ellipsis");
    truncated
}

struct TraceParentExtractor<'a> {
    parent: &'a str,
}

impl<'a> TraceParentExtractor<'a> {
    pub fn new(parent: &'a str) -> Self {
        Self { parent }
    }
}

impl<'a> Extractor for TraceParentExtractor<'_> {
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
