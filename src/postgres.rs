use std::{
    ffi::CStr,
    sync::atomic::Ordering,
    time::{Duration, SystemTime},
};

use opentelemetry::{
    Context,
    trace::{SpanContext, TraceContextExt as _},
};
use pgrx::pg_sys::{
    self,
    InstrumentOption::{INSTRUMENT_ROWS, INSTRUMENT_TIMER},
    Oid,
};

use crate::{
    DEQUE, WORKER_PID,
    config::get_otlp_traceparent,
    span::{HeaplessSpan, parse_traceparent},
};

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

pub fn collect_spans(query_desc: *mut pg_sys::QueryDesc) {
    if query_desc.is_null() {
        return;
    }

    // Use the current time to calculate the duration of the query.
    // This should be close enough to the actual end time.
    let end_time = SystemTime::now();
    let total = unsafe { (*(*query_desc).query_instr).total.ticks };
    if !meets_slow_query_threshold(total) {
        return;
    }
    let wall_start = end_time - Duration::from_nanos(total as u64);

    let parent_span = get_span_context(query_desc);
    let Some(span) = HeaplessSpan::from_query(query_desc, wall_start, &parent_span) else {
        return;
    };

    let planstate = unsafe { (*query_desc).planstate };
    collect_plan_spans(planstate, wall_start, &span);
    let _ = DEQUE.exclusive().enqueue(span);
    pg_otel_wake_worker();
}

fn meets_slow_query_threshold(duration_ns: i64) -> bool {
    let threshold_ms = unsafe { pg_sys::log_min_duration_statement };
    threshold_ms >= 0 && duration_ns >= i64::from(threshold_ms) * 1_000_000
}

pub fn collect_plan_spans(
    planstate: *mut pg_sys::PlanState,
    wall_start: SystemTime,
    parent: &HeaplessSpan,
) {
    let Some(span) = HeaplessSpan::from_plan(planstate, wall_start, parent) else {
        return;
    };

    let lefttree = unsafe { (*planstate).lefttree };
    let righttree = unsafe { (*planstate).righttree };
    if !lefttree.is_null() {
        collect_plan_spans(lefttree, wall_start, &span);
    }
    if !righttree.is_null() {
        collect_plan_spans(righttree, wall_start, &span);
    }
    let _ = DEQUE.exclusive().enqueue(span);
}

pub fn collect_table_names(state: *const pg_sys::PlanState) -> Vec<String> {
    if state.is_null() {
        return Vec::new();
    }

    let mut tables = Vec::new();
    let left_tree = unsafe { (*state).lefttree };
    for table in collect_table_names(left_tree) {
        push_unique(&mut tables, table);
    }

    let right_tree = unsafe { (*state).righttree };
    for table in collect_table_names(right_tree) {
        push_unique(&mut tables, table);
    }

    if let Some(table) = plan_table_name(state) {
        push_unique(&mut tables, table);
    }

    tables
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

pub fn plan_table_name(state: *const pg_sys::PlanState) -> Option<String> {
    let plan = unsafe { (*state).plan };
    if plan.is_null() {
        return None;
    }

    let node_type = unsafe { (*plan).type_ };
    let is_scan = matches!(
        node_type,
        pg_sys::NodeTag::T_SeqScan
            | pg_sys::NodeTag::T_SampleScan
            | pg_sys::NodeTag::T_IndexScan
            | pg_sys::NodeTag::T_IndexOnlyScan
            | pg_sys::NodeTag::T_BitmapIndexScan
            | pg_sys::NodeTag::T_BitmapHeapScan
            | pg_sys::NodeTag::T_TidScan
            | pg_sys::NodeTag::T_TidRangeScan
            | pg_sys::NodeTag::T_SubqueryScan
            | pg_sys::NodeTag::T_FunctionScan
            | pg_sys::NodeTag::T_ValuesScan
            | pg_sys::NodeTag::T_TableFuncScan
            | pg_sys::NodeTag::T_CteScan
            | pg_sys::NodeTag::T_NamedTuplestoreScan
            | pg_sys::NodeTag::T_WorkTableScan
            | pg_sys::NodeTag::T_ForeignScan
            | pg_sys::NodeTag::T_CustomScan
    );
    if !is_scan {
        return None;
    }

    // All PostgreSQL scan-state structs embed ScanState as their first field.
    let scan_state = state as *const pg_sys::ScanState;
    let relation = unsafe { (*scan_state).ss_currentRelation };
    if relation.is_null() {
        return None;
    }

    plan_table_identifier(unsafe { (*relation).rd_id })
}

pub fn plan_table_identifier(oid: Oid) -> Option<String> {
    let namespace_oid = unsafe { pg_sys::get_rel_namespace(oid) };
    let relation_name = unsafe { pg_str(pg_sys::get_rel_name(oid)) }?;
    let namespace_name = unsafe { pg_str(pg_sys::get_namespace_name_or_temp(namespace_oid)) };

    Some(match namespace_name {
        Some(namespace_name) => format!("{}.{}", namespace_name, relation_name),
        None => relation_name.to_owned(),
    })
}

pub fn pg_str<'a>(s: *const i8) -> Option<&'a str> {
    // Check utf-8 validity
    let cstr = unsafe { CStr::from_ptr(s) };
    cstr.to_str().ok()
}

/// Wake the worker after work has been added to the shared queue.
pub fn pg_otel_wake_worker() -> bool {
    let pid = WORKER_PID.get().load(Ordering::Relaxed);
    if pid == 0 {
        return false;
    }

    unsafe { libc::kill(pid, libc::SIGINT) == 0 }
}

fn get_span_context(query_desc: *const pg_sys::QueryDesc) -> SpanContext {
    if let Some(traceparent) = get_otlp_traceparent() {
        parse_traceparent(&traceparent)
    } else if let Some(traceparent) = get_traceparent_from_query_desc(query_desc) {
        parse_traceparent(traceparent)
    } else {
        Context::new().span().span_context().clone()
    }
}

struct FindIter<'a> {
    text: &'a str,
    pattern: &'a str,
    offset: usize,
}

impl<'a> FindIter<'a> {
    fn new(text: &'a str, pattern: &'a str) -> Self {
        Self {
            text,
            pattern,
            offset: 0,
        }
    }
}

impl<'a> Iterator for FindIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let text = &self.text[self.offset..];
        if let Some(start) = text.find(self.pattern) {
            self.offset += start + self.pattern.len();
            Some(self.offset - self.pattern.len())
        } else {
            None
        }
    }
}

// If string literals in the query text contain comments with traceparent values, so be it. I am not writing a full sql parser.
fn get_traceparent_from_query_desc<'a>(query_desc: *const pg_sys::QueryDesc) -> Option<&'a str> {
    let text = pg_str(unsafe { (*query_desc).sourceText })?;

    let mut starts = FindIter::new(text, "/*").peekable();
    let mut ends = FindIter::new(text, "*/").peekable();

    while let Some(start) = starts.next() {
        let mut rest = &text[start + 2..];
        // handle nested block comments (because postgres supports those, grrrr)
        while let (Some(start), Some(end)) = (starts.peek(), ends.next()) {
            if *start > end {
                rest = &rest[..end - 2];
                break;
            }
            starts.next();
        }

        for token in rest.split_ascii_whitespace() {
            if let Some(traceparent) = token.strip_prefix("pg_otel.traceparent=") {
                return Some(traceparent);
            }
        }
    }

    None
}
