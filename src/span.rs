#![allow(dead_code)]

use std::time::{Duration, SystemTime};

use opentelemetry::{SpanId, TraceId};
use pgrx::{
    log,
    pg_sys::{CmdType, NodeTag, Oid, PlanState, QueryDesc},
};

use crate::postgres::{collect_table_names, pg_str};

const QUERY_TEXT_MAX_LEN: usize = 1024;
const PLAN_NODE_NAME_MAX_LEN: usize = 64;
const PLAN_TABLES_MAX_LEN: usize = 64;

#[derive(Debug)]
pub struct HeaplessSpan {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    pub parent_id: SpanId,
    pub name: heapless::String<PLAN_NODE_NAME_MAX_LEN>,
    pub start_time: SystemTime,
    pub end_time: SystemTime,
    pub attributes: HeaplessSpanAttributes,
}

#[derive(Debug)]
pub enum HeaplessSpanAttributes {
    Query {
        operation: CmdType::Type,
        query_text: heapless::String<QUERY_TEXT_MAX_LEN>,
        exec_startup_time_seconds: f64,
        exec_total_time_seconds: f64,
    },
    PlanNode {
        plan_node_type: NodeTag,
        plan_tables: heapless::Vec<Oid, PLAN_TABLES_MAX_LEN>,
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
    },
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
            trace_id: TraceId::INVALID,
            span_id: SpanId::INVALID,
            parent_id: SpanId::INVALID,
            name,
            start_time,
            end_time,
            attributes: HeaplessSpanAttributes::Query {
                operation,
                query_text,
                exec_startup_time_seconds,
                exec_total_time_seconds,
            },
        })
    }

    pub fn from_plan(plan_node: *const PlanState, wall_start: SystemTime) -> Option<Self> {
        let Some(plan_node) = (unsafe { plan_node.as_ref() }) else {
            return None;
        };

        let Some(instrument) = (unsafe { plan_node.instrument.as_ref() }) else {
            return None;
        };

        let Some(plan) = (unsafe { plan_node.plan.as_ref() }) else {
            return None;
        };

        let name = format!("postgres.operation.{:?}", plan_node.type_);
        let name = heapless::String::try_from(name.as_str()).expect("name is bounded");

        let start_time = wall_start + Duration::from_nanos(instrument.instr.starttime.ticks as u64);
        let end_time = wall_start + Duration::from_nanos(instrument.instr.total.ticks as u64);

        let plan_table_names = collect_table_names(plan_node);
        let plan_table_len = plan_table_names.len();

        let mut plan_tables = heapless::Vec::new();
        for oid in plan_table_names {
            if plan_tables.push(oid).is_err() {
                log!("Too many plan table names: {}", plan_table_len);
            }
        }

        Some(HeaplessSpan {
            trace_id: TraceId::INVALID,
            span_id: SpanId::INVALID,
            parent_id: SpanId::INVALID,
            name,
            start_time,
            end_time,
            attributes: HeaplessSpanAttributes::PlanNode {
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
            },
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
