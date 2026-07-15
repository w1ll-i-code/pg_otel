#![allow(dead_code)]

use std::time::{Duration, SystemTime};

use opentelemetry::{
    trace::{SpanContext, SpanKind, Status, TraceState},
    Array, InstrumentationScope, KeyValue, SpanId, StringValue, TraceFlags, TraceId, Value,
};
use opentelemetry_sdk::trace::SpanData;
use pgrx::{
    log,
    pg_sys::{CmdType, NodeTag, PlanState, QueryDesc},
};

use crate::postgres::{collect_table_names, pg_str, plan_table_name};

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
    pub fn from_query(query_desc: *const QueryDesc, wall_start: SystemTime) -> Option<Self> {
        let Some(query_desc) = (unsafe { query_desc.as_ref() }) else {
            return None;
        };

        let operation = query_desc.operation;
        let name = heapless::String::try_from(query_name(operation)).unwrap();

        let Some(instrument) = (unsafe { query_desc.query_instr.as_ref() }) else {
            return None;
        };
        let start_time = wall_start + Duration::from_nanos(instrument.starttime.ticks as u64);
        let end_time = start_time + Duration::from_nanos(instrument.total.ticks as u64);

        let source_text = query_desc.sourceText;
        let query_text = pg_str(source_text).unwrap_or_default();
        let truncate = query_text.floor_char_boundary(QUERY_TEXT_MAX_LEN);
        let query_text = &query_text[..truncate];
        let query_text = heapless::String::try_from(query_text).expect("query_text was truncated.");

        let exec_startup_time_seconds = instrument.starttime.ticks as f64 / 1e9;
        let exec_total_time_seconds = instrument.total.ticks as f64 / 1e9;

        Some(HeaplessSpan {
            trace_id: TraceId::from(fastrand::u128(..)),
            span_id: SpanId::from(fastrand::u64(..)),
            parent_id: SpanId::INVALID,
            name,
            start_time,
            end_time,
            attributes: HeaplessSpanAttributes::Query(QueryAttributes {
                operation,
                query_text,
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
        let Some(plan_node) = (unsafe { plan_node.as_ref() }) else {
            return None;
        };

        let Some(instrument) = (unsafe { plan_node.instrument.as_ref() }) else {
            return None;
        };

        let Some(plan) = (unsafe { plan_node.plan.as_ref() }) else {
            return None;
        };

        let start_time = wall_start + Duration::from_nanos(instrument.instr.starttime.ticks as u64);
        let end_time = wall_start + Duration::from_nanos(instrument.instr.total.ticks as u64);

        let plan_table_names = collect_table_names(plan_node);
        let plan_table_len = plan_table_names.len();
        let table_suffix = plan_table_name(plan_node)
            .map(|table| format!(" [{}]", table))
            .unwrap_or_default();
        let name = format!("postgres.operation.{:?}{}", plan_node.type_, table_suffix);
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
            span_id: SpanId::from(fastrand::u64(..)),
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
            TraceFlags::SAMPLED,
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
                    parent_span_is_remote: true,
                    span_kind: SpanKind::Server,
                    name: span.name.as_str().to_owned().into(),
                    start_time: span.start_time,
                    end_time: span.end_time,
                    attributes: vec![
                        KeyValue::new("db.system.name", "postgresql"),
                        KeyValue::new("db.operation.name", operation),
                        KeyValue::new("db.query.text", query_text),
                        KeyValue::new(
                            "postgresql.execution.startup_time_seconds",
                            attr.exec_startup_time_seconds,
                        ),
                        KeyValue::new(
                            "postgresql.execution.total_time_seconds",
                            attr.exec_total_time_seconds,
                        ),
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
