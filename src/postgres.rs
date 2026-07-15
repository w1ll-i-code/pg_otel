use std::{
    ffi::CStr,
    time::{Duration, SystemTime},
};

use pgrx::pg_sys::{
    self,
    InstrumentOption::{INSTRUMENT_ROWS, INSTRUMENT_TIMER},
    Oid,
};

use crate::{span::HeaplessSpan, DEQUE};

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
    let wall_start = end_time - Duration::from_nanos(total as u64);

    if let Some(span) = HeaplessSpan::from_query(query_desc, wall_start) {
        let _ = DEQUE.exclusive().enqueue(span);
    }

    let planstate = unsafe { (*query_desc).planstate };
    collect_plan_spans(planstate, wall_start);
}

pub fn collect_plan_spans(planstate: *mut pg_sys::PlanState, wall_start: SystemTime) {
    if let Some(span) = HeaplessSpan::from_plan(planstate, wall_start) {
        let _ = DEQUE.exclusive().enqueue(span);
    }

    let lefttree = unsafe { (*planstate).lefttree };
    let righttree = unsafe { (*planstate).righttree };
    if !lefttree.is_null() {
        collect_plan_spans(lefttree, wall_start);
    }
    if !righttree.is_null() {
        collect_plan_spans(righttree, wall_start);
    }
}

pub fn collect_table_names(state: *const pg_sys::PlanState) -> Vec<Oid> {
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

    if let Some(table) = relation_name(state) {
        push_unique(&mut tables, table);
    }

    tables
}

fn push_unique(values: &mut Vec<Oid>, value: Oid) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn relation_name(state: *const pg_sys::PlanState) -> Option<Oid> {
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

    Some(unsafe { (*relation).rd_id })
}

pub fn pg_str<'a>(s: *const i8) -> Option<&'a str> {
    // Check utf-8 validity
    let cstr = unsafe { CStr::from_ptr(s) };
    cstr.to_str().ok()
}
