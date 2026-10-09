use std::{
    borrow::Cow,
    collections::{BTreeSet, HashSet},
    ffi::CStr,
    panic::AssertUnwindSafe,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, SystemTime},
};

use opentelemetry::trace::SpanContext;
use pgrx::{
    PgSqlErrorCode, PgTryBuilder, debug1, log,
    pg_sys::{self, Oid},
};

use crate::{
    codec::encode_batch,
    compat,
    config::{get_max_plan_spans, get_min_duration_ms, get_otlp_traceparent, get_query_text_mode},
    shared,
    span::{
        ChildEdge, ParentContext, PlanChild, Relationship, SpanLink, SpanRecord, parse_traceparent,
        random_span_id, walk_plan,
    },
};

/// Whether statements run by this backend are traced.
///
/// Tracing needs the GUC to be enabled (`-1` disables it) and a backend that
/// is not a parallel worker: the leader already reports the whole statement
/// and the workers' instrumentation is merged into the leader's.
fn tracing_enabled(min_duration_ms: i32, parallel_worker_number: i32) -> bool {
    min_duration_ms >= 0 && parallel_worker_number < 0
}

fn tracing_enabled_here() -> bool {
    // SAFETY: plain reads of backend-local variables.
    tracing_enabled(get_min_duration_ms(), unsafe {
        pg_sys::ParallelWorkerNumber
    })
}

/// `EXPLAIN` without `ANALYZE` plans and starts the executor but never runs
/// it, so there is nothing to report.
fn is_explain_only(eflags: i32) -> bool {
    eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as i32 != 0
}

/// Whether the executor should be asked for instrumentation for this statement.
fn wants_instrumentation(query_desc: *mut pg_sys::QueryDesc, eflags: i32) -> bool {
    !query_desc.is_null() && !is_explain_only(eflags) && tracing_enabled_here()
}

/// Asks the executor to collect the per-node instrumentation needed for spans.
/// Call before `ExecutorStart`.
///
/// Does nothing when tracing is disabled or for `EXPLAIN` without `ANALYZE`,
/// so such statements pay no instrumentation overhead.
pub fn request_instrumentation(query_desc: *mut pg_sys::QueryDesc, eflags: i32) {
    if wants_instrumentation(query_desc, eflags) {
        // SAFETY: non-null `query_desc` of the statement about to start.
        unsafe { compat::request_instrumentation_before_start(query_desc) };
    }
}

/// The part of the instrumentation request that only works once the executor
/// state exists (statement timing on PG18). Call after `ExecutorStart`, with the
/// same arguments as [`request_instrumentation`].
pub fn complete_instrumentation_request(query_desc: *mut pg_sys::QueryDesc, eflags: i32) {
    if wants_instrumentation(query_desc, eflags) {
        // SAFETY: non-null `query_desc` whose ExecutorStart has completed.
        unsafe { compat::request_instrumentation_after_start(query_desc) };
    }
}

/// Collects and enqueues the spans of a finished statement.
///
/// Telemetry must never fail the user's query: Rust panics and Postgres errors
/// raised while collecting are caught, reported once per backend at `LOG`
/// level (afterwards at `DEBUG1`) and the spans of this statement are dropped.
/// A query cancel (`57014`) is the exception: it is re-raised so the user's
/// cancel request is honoured instead of being swallowed.
///
/// This is best-effort containment: errors that Postgres escalates (`FATAL`,
/// `PANIC`) or that leave shared state inconsistent cannot be contained.
pub fn collect_spans(query_desc: *mut pg_sys::QueryDesc) {
    // SAFETY: only reads a global; restored below because an error may leave
    // Postgres in a different memory context.
    let saved_context = unsafe { pg_sys::CurrentMemoryContext };
    PgTryBuilder::new(AssertUnwindSafe(|| collect_spans_unguarded(query_desc)))
        .catch_when(PgSqlErrorCode::ERRCODE_QUERY_CANCELED, |error| {
            error.rethrow()
        })
        .catch_others(|error| {
            report_collection_failure(&error);
        })
        .execute();
    // SAFETY: `saved_context` was current when we started and is still alive.
    unsafe { pg_sys::CurrentMemoryContext = saved_context };
}

static COLLECTION_FAILURE_LOGGED: AtomicBool = AtomicBool::new(false);

fn report_collection_failure(error: &pgrx::pg_sys::panic::CaughtError) {
    use pgrx::pg_sys::panic::CaughtError;
    let message = match error {
        CaughtError::PostgresError(report) | CaughtError::ErrorReport(report) => report.message(),
        CaughtError::RustPanic { ereport, .. } => ereport.message(),
    };
    if COLLECTION_FAILURE_LOGGED.swap(true, Ordering::Relaxed) {
        debug1!("pg_otel: dropping spans after collection failure: {message}");
    } else {
        log!(
            "pg_otel: dropping spans after collection failure (further failures logged at DEBUG1): {message}"
        );
    }
}

fn collect_spans_unguarded(query_desc: *mut pg_sys::QueryDesc) {
    if query_desc.is_null() || !tracing_enabled_here() {
        return;
    }
    fault::inject();

    // SAFETY: `query_desc` is non-null and valid for the duration of the hook;
    // `estate` is set by ExecutorStart.
    let explain_only = unsafe {
        let estate = (*query_desc).estate;
        !estate.is_null() && is_explain_only((*estate).es_top_eflags)
    };
    if explain_only {
        return;
    }

    // Use the current time to calculate the start of the query. This is close
    // enough to the actual end time. The measured total is the time spent in
    // ExecutorRun and ExecutorFinish (not ExecutorStart/End). There is none when
    // instrumentation was not requested (tracing was disabled at ExecutorStart).
    let end_time = SystemTime::now();
    // SAFETY: `query_desc` is non-null and valid in ExecutorEnd.
    let Some(total_ns) = (unsafe { compat::query_total_ns(&*query_desc) }) else {
        return;
    };
    if !meets_slow_query_threshold(total_ns, get_min_duration_ms()) {
        return;
    }
    let wall_start = end_time - Duration::from_nanos(total_ns as u64);

    let source_text = pg_str(unsafe { (*query_desc).sourceText });
    let guc_traceparent = get_otlp_traceparent();
    let parent = match decide_parent(guc_traceparent.as_deref(), source_text.as_deref()) {
        TraceDecision::Skip => return,
        TraceDecision::Trace(parent) => parent,
    };
    // Everything below builds the spans of the statement in local memory. It
    // can call into Postgres (relation names, query text sanitizing), which is
    // only allowed while no lock is held; the queue lock is taken once, at the
    // very end, by `publish_spans`.
    let query_span_id = random_span_id();
    let query_link = SpanLink {
        trace_id: parent.trace_id,
        span_id: query_span_id,
        end_time: wall_start + Duration::from_nanos(total_ns as u64),
    };
    let planstate = unsafe { (*query_desc).planstate };
    let PlanSpans {
        mut spans,
        tables,
        omitted,
    } = collect_plan_spans(planstate, wall_start, &query_link, get_max_plan_spans());

    let Some(query_span) = SpanRecord::from_query(
        query_desc,
        wall_start,
        &parent,
        get_query_text_mode(),
        query_span_id,
        &tables,
        omitted,
    ) else {
        return;
    };
    spans.push(query_span);
    publish_spans(spans);
}

/// `-1` disables tracing, `0` traces every statement.
fn meets_slow_query_threshold(duration_ns: i64, min_duration_ms: i32) -> bool {
    min_duration_ms >= 0 && duration_ns >= i64::from(min_duration_ms) * 1_000_000
}

/// Hands all spans of one statement to the exporter worker, or drops them all
/// (counting them) when the queue is full.
///
/// Encoding happens first, without any lock; the queue lock is then held only
/// for a single memory copy. Never call Postgres code while holding that lock:
/// if the code raised an ERROR that we catch, the LWLock would leak (pgrx and
/// [`shared`] only release it on unwind when InterruptHoldoffCount is
/// non-zero), and every later publish would hang.
fn publish_spans(spans: Vec<SpanRecord>) {
    let Some(spans) = capture::divert(spans) else {
        return;
    };
    let (batch, skipped) = encode_batch(&spans);
    shared::publish(&batch, skipped);
}

/// The spans of a plan tree.
struct PlanSpans {
    /// Parents come before their children.
    spans: Vec<SpanRecord>,
    /// Relations scanned by the nodes that got a span.
    tables: BTreeSet<String>,
    /// Plan nodes without a span because `max_spans` was reached.
    omitted: usize,
}

/// Builds a span for every node of the plan tree below `planstate`, like
/// EXPLAIN walks it (see [`plan_state_children`]), but at most `max_spans`.
/// Nodes beyond the limit are counted, not exported; being depth-first, the
/// walk drops the last subtrees and keeps the top of the plan connected.
fn collect_plan_spans(
    planstate: *mut pg_sys::PlanState,
    wall_start: SystemTime,
    query: &SpanLink,
    max_spans: usize,
) -> PlanSpans {
    let mut result = PlanSpans {
        spans: Vec::new(),
        tables: BTreeSet::new(),
        omitted: 0,
    };
    if planstate.is_null() {
        return result;
    }

    // EXPLAIN prints a physical sub-plan once even if several SubPlan nodes
    // reference it; the set is global to the walk, like `printed_subplans`.
    let mut printed_subplans = HashSet::new();
    let summary = walk_plan(
        planstate,
        max_spans,
        // SAFETY: nodes come from the executor tree of the finished statement.
        |&node| unsafe { plan_state_children(node, &mut printed_subplans) },
        |&node, edge, parent| {
            let span = SpanRecord::from_plan(node, wall_start, &parent.unwrap_or(*query), edge)?;
            if let Some(relation) = span.relation() {
                result.tables.insert(relation.to_owned());
            }
            let link = span.link();
            result.spans.push(span);
            Some(link)
        },
    );
    result.omitted = summary.omitted;
    result
}

/// The child plan states of `node`, in the order and with the relationships
/// EXPLAIN shows them (`ExplainNode`, and `planstate_tree_walker` for which
/// children exist): initPlans, outer, inner, the members of Append /
/// MergeAppend / BitmapAnd / BitmapOr (only the live ones after run-time
/// pruning), the sub-select of a SubqueryScan, the children of a CustomScan,
/// then subPlans. ModifyTable needs no special case: its input is the outer
/// plan.
///
/// Sub-plans already reported through another node are skipped
/// (`printed_subplans`, keyed by `plan_id`).
///
/// # Safety
///
/// `node` must be a valid, initialised plan state.
unsafe fn plan_state_children(
    node: *mut pg_sys::PlanState,
    printed_subplans: &mut HashSet<i32>,
) -> Vec<PlanChild<*mut pg_sys::PlanState>> {
    let mut children = Vec::new();
    // SAFETY: valid per the function contract; the tag of the plan says which
    // concrete state type `node` is embedded in, as in Postgres' own walker.
    unsafe {
        let state = &*node;
        push_subplans(
            &mut children,
            state.initPlan,
            Relationship::InitPlan,
            printed_subplans,
        );
        push_child(&mut children, state.lefttree, Relationship::Outer);
        push_child(&mut children, state.righttree, Relationship::Inner);

        if let Some(plan) = state.plan.as_ref() {
            match plan.type_ {
                pg_sys::NodeTag::T_Append => {
                    let append = &*node.cast::<pg_sys::AppendState>();
                    push_members(&mut children, append.appendplans, append.as_nplans);
                }
                pg_sys::NodeTag::T_MergeAppend => {
                    let merge = &*node.cast::<pg_sys::MergeAppendState>();
                    push_members(&mut children, merge.mergeplans, merge.ms_nplans);
                }
                pg_sys::NodeTag::T_BitmapAnd => {
                    let bitmap = &*node.cast::<pg_sys::BitmapAndState>();
                    push_members(&mut children, bitmap.bitmapplans, bitmap.nplans);
                }
                pg_sys::NodeTag::T_BitmapOr => {
                    let bitmap = &*node.cast::<pg_sys::BitmapOrState>();
                    push_members(&mut children, bitmap.bitmapplans, bitmap.nplans);
                }
                pg_sys::NodeTag::T_SubqueryScan => {
                    let scan = &*node.cast::<pg_sys::SubqueryScanState>();
                    push_child(&mut children, scan.subplan, Relationship::Subquery);
                }
                pg_sys::NodeTag::T_CustomScan => {
                    let custom = &*node.cast::<pg_sys::CustomScanState>();
                    for child in list_pointers::<pg_sys::PlanState>(custom.custom_ps) {
                        push_child(&mut children, child, Relationship::Child);
                    }
                }
                _ => {}
            }
        }

        push_subplans(
            &mut children,
            state.subPlan,
            Relationship::SubPlan,
            printed_subplans,
        );
    }
    children
}

fn push_child(
    children: &mut Vec<PlanChild<*mut pg_sys::PlanState>>,
    node: *mut pg_sys::PlanState,
    relationship: Relationship,
) {
    if !node.is_null() {
        children.push(PlanChild {
            node,
            edge: ChildEdge::new(relationship),
        });
    }
}

/// # Safety
///
/// `members` must point to `count` valid plan state pointers (or be null).
unsafe fn push_members(
    children: &mut Vec<PlanChild<*mut pg_sys::PlanState>>,
    members: *mut *mut pg_sys::PlanState,
    count: i32,
) {
    if members.is_null() {
        return;
    }
    for index in 0..usize::try_from(count).unwrap_or(0) {
        // SAFETY: `index < count` per the function contract.
        push_child(
            children,
            unsafe { *members.add(index) },
            Relationship::Member,
        );
    }
}

/// Adds the plan states of a `List` of `SubPlanState`s (initPlan / subPlan).
///
/// # Safety
///
/// `list` must be null or a valid list of `SubPlanState` pointers.
unsafe fn push_subplans(
    children: &mut Vec<PlanChild<*mut pg_sys::PlanState>>,
    list: *mut pg_sys::List,
    relationship: Relationship,
    printed_subplans: &mut HashSet<i32>,
) {
    // SAFETY: per the function contract.
    for subplan_state in unsafe { list_pointers::<pg_sys::SubPlanState>(list) } {
        let Some(subplan_state) = (unsafe { subplan_state.as_ref() }) else {
            continue;
        };
        // SAFETY: a SubPlanState points to its SubPlan.
        let Some(subplan) = (unsafe { subplan_state.subplan.as_ref() }) else {
            continue;
        };
        if subplan_state.planstate.is_null() || !printed_subplans.insert(subplan.plan_id) {
            continue;
        }
        let name = compat::subplan_display_name(subplan);
        children.push(PlanChild {
            node: subplan_state.planstate,
            edge: ChildEdge {
                relationship,
                subplan_name: Some(name),
            },
        });
    }
}

/// The pointers stored in a `List` of pointers (empty for a null list).
///
/// # Safety
///
/// `list` must be null or a valid `List` holding `*mut T` pointers.
unsafe fn list_pointers<T>(list: *mut pg_sys::List) -> Vec<*mut T> {
    // SAFETY: per the function contract.
    let Some(list) = (unsafe { list.as_ref() }) else {
        return Vec::new();
    };
    (0..usize::try_from(list.length).unwrap_or(0))
        // SAFETY: `elements` has `length` valid cells for a non-empty list.
        .map(|index| unsafe { (*list.elements.add(index)).ptr_value.cast::<T>() })
        .collect()
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
        Some(namespace_name) => format!("{namespace_name}.{relation_name}"),
        None => relation_name.into_owned(),
    })
}

/// Reads a NUL-terminated string from Postgres, replacing invalid UTF-8 with
/// U+FFFD. Returns `None` for a null pointer.
pub fn pg_str<'a>(s: *const i8) -> Option<Cow<'a, str>> {
    if s.is_null() {
        return None;
    }
    // SAFETY: non-null; Postgres strings are NUL-terminated and outlive the hook.
    let cstr = unsafe { CStr::from_ptr(s) };
    Some(cstr.to_string_lossy())
}

/// What to do with a statement once its parent trace is known.
#[derive(Debug, PartialEq, Eq)]
enum TraceDecision {
    /// The caller's trace is not sampled: export nothing.
    Skip,
    Trace(ParentContext),
}

/// Picks the parent trace of a statement.
///
/// Precedence: a valid `pg_otel.traceparent` GUC value, then the first valid
/// `traceparent` found in a comment of the query text, otherwise a new trace.
/// Invalid values are ignored rather than reported. A valid but unsampled
/// remote parent suppresses tracing of the statement.
fn decide_parent(guc_traceparent: Option<&str>, query_text: Option<&str>) -> TraceDecision {
    let remote = guc_traceparent
        .and_then(parse_traceparent)
        .or_else(|| query_text.and_then(traceparent_from_comments));
    match remote {
        None => TraceDecision::Trace(ParentContext::new_root()),
        Some(remote) if !remote.is_sampled() => TraceDecision::Skip,
        Some(remote) => TraceDecision::Trace(ParentContext::from_remote(&remote)),
    }
}

/// First valid trace context carried by a comment of `text`.
fn traceparent_from_comments(text: &str) -> Option<SpanContext> {
    // Cheap pre-check: most statements carry no traceparent at all. Keys are
    // matched case-insensitively, so the pre-check must be as well.
    const KEY: &[u8] = b"traceparent";
    if !text
        .as_bytes()
        .windows(KEY.len())
        .any(|window| window.eq_ignore_ascii_case(KEY))
    {
        return None;
    }
    block_comments(text)
        .into_iter()
        .flat_map(traceparent_values)
        .find_map(|value| parse_traceparent(&value))
}

/// Inner text of every complete `/* ... */` comment in `text`, including
/// comments nested inside others (Postgres allows nesting). Inner comments come
/// before the comment that contains them. Unterminated comments and stray `*/`
/// are ignored.
///
/// This is a plain text scan, not a SQL lexer: a comment marker inside a string
/// literal or a `--` line comment is treated as a comment. A user who can put
/// such text in a statement could already set `pg_otel.traceparent` themselves,
/// so this over-matching is accepted.
fn block_comments(text: &str) -> Vec<&str> {
    let bytes = text.as_bytes();
    let mut comments = Vec::new();
    let mut open = Vec::new();
    let mut i = 0;
    while i + 1 < bytes.len() {
        match (bytes[i], bytes[i + 1]) {
            (b'/', b'*') => {
                open.push(i + 2);
                i += 2;
            }
            (b'*', b'/') => {
                if let Some(start) = open.pop() {
                    // `start` and `i` follow/precede ASCII bytes, so both are
                    // character boundaries.
                    comments.push(&text[start..i]);
                }
                i += 2;
            }
            _ => i += 1,
        }
    }
    comments
}

/// Values of `traceparent` / `pg_otel.traceparent` keys in one comment.
///
/// Understands `key=value` pairs separated by commas and/or whitespace, with
/// the value optionally in single or double quotes, which covers both the
/// sqlcommenter format (`traceparent='00-...-01'`) and
/// `pg_otel.traceparent=00-...-01`. Keys and values are percent-decoded.
/// Unquoted values end at whitespace or a comma; an unterminated quoted value
/// is dropped.
fn traceparent_values(comment: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut rest = comment;
    loop {
        rest = rest.trim_start_matches(|c: char| c.is_whitespace() || c == ',');
        let Some(first) = rest.chars().next() else {
            break;
        };
        let key_end = rest
            .find(|c: char| c.is_whitespace() || matches!(c, '=' | ',' | '\'' | '"'))
            .unwrap_or(rest.len());
        if key_end == 0 {
            // Stray `=` or quote: skip it so the scan always advances.
            rest = &rest[first.len_utf8()..];
            continue;
        }
        let (key, after_key) = rest.split_at(key_end);
        let Some(after_equals) = after_key.trim_start().strip_prefix('=') else {
            rest = after_key;
            continue;
        };
        let (value, remaining) = split_value(after_equals.trim_start());
        if is_traceparent_key(&percent_decode(key)) && !value.is_empty() {
            values.push(percent_decode(value));
        }
        rest = remaining;
    }
    values
}

fn is_traceparent_key(key: &str) -> bool {
    key.eq_ignore_ascii_case("traceparent") || key.eq_ignore_ascii_case("pg_otel.traceparent")
}

/// Splits a leading (optionally quoted) value from the text after it. Returns
/// an empty value for an unterminated quote.
fn split_value(text: &str) -> (&str, &str) {
    if let Some(quote) = text.chars().next().filter(|c| matches!(c, '\'' | '"')) {
        let body = &text[1..];
        return match body.find(quote) {
            Some(end) => (&body[..end], &body[end + 1..]),
            None => ("", ""),
        };
    }
    let end = text
        .find(|c: char| c.is_whitespace() || c == ',')
        .unwrap_or(text.len());
    text.split_at(end)
}

/// Decodes `%XX` escapes. Malformed escapes are kept literally and invalid
/// UTF-8 in the result is replaced.
fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let escaped = (bytes[i] == b'%')
            .then(|| bytes.get(i + 1..i + 3))
            .flatten()
            .and_then(|hex| {
                let high = (hex[0] as char).to_digit(16)?;
                let low = (hex[1] as char).to_digit(16)?;
                Some((high * 16 + low) as u8)
            });
        match escaped {
            Some(byte) => {
                decoded.push(byte);
                i += 3;
            }
            None => {
                decoded.push(bytes[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

/// Test-only capture of published spans, so tests can inspect exactly what
/// would be exported without racing the background worker that drains the
/// shared queue.
#[cfg(any(test, feature = "pg_test"))]
pub mod capture {
    use std::cell::RefCell;

    use crate::span::SpanRecord;

    thread_local! {
        static CAPTURED: RefCell<Option<Vec<SpanRecord>>> = const { RefCell::new(None) };
    }

    /// Starts diverting published spans (instead of queueing them).
    pub fn start() {
        CAPTURED.with(|captured| *captured.borrow_mut() = Some(Vec::new()));
    }

    /// Stops diverting and returns the spans captured since [`start`].
    pub fn finish() -> Vec<SpanRecord> {
        CAPTURED.with(|captured| captured.borrow_mut().take().unwrap_or_default())
    }

    /// Returns the batch back when nothing is capturing.
    pub(super) fn divert(batch: Vec<SpanRecord>) -> Option<Vec<SpanRecord>> {
        CAPTURED.with(|captured| match captured.borrow_mut().as_mut() {
            Some(spans) => {
                spans.extend(batch);
                None
            }
            None => Some(batch),
        })
    }
}

#[cfg(not(any(test, feature = "pg_test")))]
mod capture {
    use crate::span::SpanRecord;

    #[inline(always)]
    pub(super) fn divert(batch: Vec<SpanRecord>) -> Option<Vec<SpanRecord>> {
        Some(batch)
    }
}

/// Test-only fault injection into the guarded collection path, to prove that
/// telemetry failures never reach the user's query. Compiled out of release
/// builds; each armed fault fires once.
#[cfg(any(test, feature = "pg_test"))]
mod fault {
    use std::sync::atomic::{AtomicU8, Ordering};

    use pgrx::{PgSqlErrorCode, ereport, pg_sys};

    #[derive(Clone, Copy)]
    #[repr(u8)]
    pub enum Fault {
        /// A genuine error raised from C code (longjmp turned into a panic).
        PostgresError = 1,
        /// A Rust panic.
        Panic = 2,
        /// A query cancel, which must be re-raised.
        QueryCancel = 3,
    }

    static ARMED: AtomicU8 = AtomicU8::new(0);

    pub fn arm(fault: Fault) {
        ARMED.store(fault as u8, Ordering::Relaxed);
    }

    pub fn inject() {
        match ARMED.swap(0, Ordering::Relaxed) {
            1 => {
                // SAFETY: valid NUL-terminated input; the call raises ERROR.
                unsafe { pg_sys::pg_strtoint32(c"not a number".as_ptr()) };
            }
            2 => panic!("injected panic"),
            3 => {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_QUERY_CANCELED,
                    "injected query cancel"
                );
            }
            _ => {}
        }
    }
}

#[cfg(not(any(test, feature = "pg_test")))]
mod fault {
    #[inline(always)]
    pub fn inject() {}
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use opentelemetry::trace::TraceId;

    const TP: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    const TP_UNSAMPLED: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";
    const OTHER_TP: &str = "00-11111111111111111111111111111111-2222222222222222-01";

    fn found(text: &str) -> Option<String> {
        traceparent_from_comments(text).map(|c| format!("{}", c.trace_id()))
    }

    fn trace_hex(tp: &str) -> String {
        parse_traceparent(tp).unwrap().trace_id().to_string()
    }

    #[test]
    fn extracts_pg_otel_key_with_spaces() {
        let text = format!("SELECT 1 /* pg_otel.traceparent={TP} */");
        assert_eq!(found(&text), Some(trace_hex(TP)));
    }

    #[test]
    fn extracts_pg_otel_key_without_spaces() {
        let text = format!("SELECT 1 /*pg_otel.traceparent={TP}*/");
        assert_eq!(found(&text), Some(trace_hex(TP)));
    }

    #[test]
    fn extracts_leading_comment() {
        let text = format!("/* pg_otel.traceparent={TP} */ SELECT 1");
        assert_eq!(found(&text), Some(trace_hex(TP)));
    }

    #[test]
    fn extracts_sqlcommenter_quoted_value() {
        let text = format!("SELECT 1 /*traceparent='{TP}'*/");
        assert_eq!(found(&text), Some(trace_hex(TP)));
    }

    #[test]
    fn extracts_sqlcommenter_among_other_pairs() {
        let text = format!(
            "SELECT 1 /*controller='users%2Fshow',framework='rails',traceparent='{TP}',route='%2Fusers'*/"
        );
        assert_eq!(found(&text), Some(trace_hex(TP)));
    }

    #[test]
    fn percent_decodes_value() {
        let encoded = TP.replace('-', "%2D");
        let text = format!("SELECT 1 /*traceparent='{encoded}'*/");
        assert_eq!(found(&text), Some(trace_hex(TP)));
    }

    #[test]
    fn accepts_double_quotes_and_spaces_around_equals() {
        let text = format!("SELECT 1 /* pg_otel.traceparent = \"{TP}\" */");
        assert_eq!(found(&text), Some(trace_hex(TP)));
    }

    #[test]
    fn multiple_comments_do_not_panic_and_first_valid_wins() {
        // Regression: this used to panic while slicing.
        assert_eq!(found("SELECT 1 FROM t WHERE x = 1 /* a */ /* b */"), None);
        let text = format!(
            "SELECT 1 /* a */ /* pg_otel.traceparent=bogus */ /* pg_otel.traceparent={TP} */ /* traceparent='{OTHER_TP}' */"
        );
        assert_eq!(found(&text), Some(trace_hex(TP)));
    }

    #[test]
    fn nested_comments_are_scanned() {
        let text = format!("SELECT 1 /* outer /* pg_otel.traceparent={TP} */ tail */");
        assert_eq!(found(&text), Some(trace_hex(TP)));
        let text = format!("SELECT 1 /* outer /*pg_otel.traceparent={TP}*/*/");
        assert_eq!(found(&text), Some(trace_hex(TP)));
    }

    #[test]
    fn does_not_include_comment_terminator_in_value() {
        let values = traceparent_values(&format!("pg_otel.traceparent={TP}"));
        assert_eq!(values, vec![TP.to_owned()]);
    }

    #[test]
    fn unterminated_comment_is_ignored() {
        let text = format!("SELECT 1 /* pg_otel.traceparent={TP}");
        assert_eq!(found(&text), None);
        assert_eq!(found("SELECT 1 /* traceparent='"), None);
        assert_eq!(found("/*"), None);
        assert_eq!(found("*/ traceparent=x"), None);
    }

    #[test]
    fn unterminated_quote_is_dropped() {
        assert!(traceparent_values(&format!("traceparent='{TP}")).is_empty());
    }

    #[test]
    fn keys_are_case_insensitive() {
        let text = format!("SELECT 1 /* PG_OTEL.TraceParent={TP} */");
        assert_eq!(found(&text), Some(trace_hex(TP)));
        let text = format!("SELECT 1 /*TRACEPARENT='{TP}'*/");
        assert_eq!(found(&text), Some(trace_hex(TP)));
    }

    #[test]
    fn non_ascii_text_does_not_panic() {
        let text = format!("SELECT 'é€😀' /* héllo 😀 pg_otel.traceparent={TP} ünï */ -- ¿");
        assert_eq!(found(&text), Some(trace_hex(TP)));
        assert_eq!(found("/*😀*/ /*traceparent='😀'*/ 😀"), None);
        assert_eq!(found("/*é"), None);
        assert_eq!(found("é*/é/*"), None);
    }

    #[test]
    fn invalid_values_are_ignored() {
        for text in [
            "SELECT 1 /* pg_otel.traceparent= */",
            "SELECT 1 /* pg_otel.traceparent */",
            "SELECT 1 /* traceparent='' */",
            "SELECT 1 /* traceparent='not-a-traceparent' */",
            "SELECT 1 /* traceparent='00-00000000000000000000000000000000-00f067aa0ba902b7-01' */",
            "SELECT 1 /* other=1 */",
            "SELECT 1 /* = ' \" , */",
        ] {
            assert_eq!(found(text), None, "{text}");
        }
    }

    #[test]
    fn comment_marker_inside_string_literal_is_matched() {
        // Documented over-match: this is a plain text scan, not a SQL lexer.
        let text = format!("SELECT '/* pg_otel.traceparent={TP} */'");
        assert_eq!(found(&text), Some(trace_hex(TP)));
    }

    #[test]
    fn key_outside_comment_is_ignored() {
        let text = format!("SELECT 'pg_otel.traceparent={TP}'");
        assert_eq!(found(&text), None);
    }

    #[test]
    fn percent_decode_handles_malformed_escapes() {
        assert_eq!(percent_decode("a%2Fb"), "a/b");
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%zz%4"), "%zz%4");
        assert_eq!(percent_decode("%ff"), "\u{fffd}");
        assert_eq!(percent_decode("%C3%A9"), "é");
    }

    #[test]
    fn guc_takes_precedence_over_comment() {
        let text = format!("SELECT 1 /* traceparent='{OTHER_TP}' */");
        let TraceDecision::Trace(parent) = decide_parent(Some(TP), Some(&text)) else {
            panic!("expected trace");
        };
        assert_eq!(parent.trace_id.to_string(), trace_hex(TP));
        assert!(parent.is_remote);
    }

    #[test]
    fn invalid_guc_falls_back_to_comment() {
        let text = format!("SELECT 1 /* traceparent='{OTHER_TP}' */");
        for guc in [None, Some(""), Some("bogus")] {
            let TraceDecision::Trace(parent) = decide_parent(guc, Some(&text)) else {
                panic!("expected trace");
            };
            assert_eq!(parent.trace_id.to_string(), trace_hex(OTHER_TP));
            assert!(parent.is_remote);
        }
    }

    #[test]
    fn no_parent_starts_new_trace() {
        let TraceDecision::Trace(parent) = decide_parent(None, Some("SELECT 1")) else {
            panic!("expected trace");
        };
        assert!(!parent.is_remote);
        assert_ne!(parent.trace_id, TraceId::INVALID);
        assert_eq!(parent.span_id, opentelemetry::SpanId::INVALID);
        assert!(matches!(decide_parent(None, None), TraceDecision::Trace(_)));
    }

    #[test]
    fn unsampled_parent_skips_tracing() {
        assert_eq!(decide_parent(Some(TP_UNSAMPLED), None), TraceDecision::Skip);
        let text = format!("SELECT 1 /* traceparent='{TP_UNSAMPLED}' */");
        assert_eq!(decide_parent(None, Some(&text)), TraceDecision::Skip);
    }

    #[test]
    fn threshold_semantics() {
        assert!(!meets_slow_query_threshold(10_000_000_000, -1));
        assert!(meets_slow_query_threshold(0, 0));
        assert!(!meets_slow_query_threshold(999_999, 1));
        assert!(meets_slow_query_threshold(1_000_000, 1));
        assert!(meets_slow_query_threshold(i64::MAX, i32::MAX));
    }

    #[test]
    fn explain_only_flag_is_detected() {
        assert!(is_explain_only(pg_sys::EXEC_FLAG_EXPLAIN_ONLY as i32));
        assert!(is_explain_only(
            pg_sys::EXEC_FLAG_EXPLAIN_ONLY as i32 | 0x10
        ));
        assert!(!is_explain_only(0));
    }

    #[test]
    fn tracing_disabled_when_off_or_in_parallel_worker() {
        assert!(!tracing_enabled(-1, -1));
        assert!(tracing_enabled(0, -1));
        assert!(!tracing_enabled(0, 0));
        assert!(!tracing_enabled(100, 3));
    }
}

// End-to-end checks that tracing never turns into a query error. They run the
// real executor hooks (the harness preloads the extension). The module must be
// called `tests`: pgrx looks the test functions up in that schema.
#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use opentelemetry::Value;
    use opentelemetry_sdk::trace::SpanData;
    use pgrx::prelude::*;

    use super::{
        capture,
        fault::{self, Fault},
    };

    const TP: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    const TP_UNSAMPLED: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";

    /// Runs `sql` with tracing of every statement enabled and returns the spans
    /// that would have been exported for it.
    fn traced_spans(sql: &str) -> Vec<SpanData> {
        Spi::run("SET pg_otel.min_duration_ms = 0").unwrap();
        capture::start();
        let result = Spi::run(sql);
        let spans = capture::finish();
        result.unwrap();
        spans.into_iter().map(SpanData::from).collect()
    }

    fn attr<'a>(span: &'a SpanData, key: &str) -> Option<&'a Value> {
        span.attributes
            .iter()
            .find(|kv| kv.key.as_str() == key)
            .map(|kv| &kv.value)
    }

    fn number(span: &SpanData, key: &str) -> f64 {
        match attr(span, key) {
            Some(Value::F64(value)) => *value,
            Some(Value::I64(value)) => *value as f64,
            other => panic!("{key} is not a number: {other:?}"),
        }
    }

    fn query_span(spans: &[SpanData]) -> &SpanData {
        let mut roots = spans
            .iter()
            .filter(|s| attr(s, "db.operation.name").is_some());
        let root = roots.next().expect("a query span");
        assert!(roots.next().is_none(), "exactly one query span expected");
        root
    }

    fn query_text(span: &SpanData) -> Option<String> {
        attr(span, "db.query.text").map(|v| v.as_str().into_owned())
    }

    #[pg_test]
    fn normalized_query_text_hides_literals() {
        let spans = traced_spans("SELECT 'secret_value_xyz'::text");
        let text = query_text(query_span(&spans)).expect("query text");
        assert!(!text.contains("secret_value_xyz"), "{text}");
        assert!(text.contains("$1"), "{text}");
    }

    #[pg_test]
    fn raw_query_text_is_exported_as_is() {
        Spi::run("SET pg_otel.query_text = 'raw'").unwrap();
        let spans = traced_spans("SELECT 'secret_value_xyz'::text");
        let text = query_text(query_span(&spans)).expect("query text");
        assert!(text.contains("secret_value_xyz"), "{text}");
    }

    #[pg_test]
    fn off_query_text_exports_no_text() {
        Spi::run("SET pg_otel.query_text = 'off'").unwrap();
        let spans = traced_spans("SELECT 'secret_value_xyz'::text");
        assert!(attr(query_span(&spans), "db.query.text").is_none());
        for span in &spans {
            assert!(!format!("{:?}", span.attributes).contains("secret_value_xyz"));
        }
    }

    #[pg_test]
    fn query_text_is_longer_than_before_but_still_bounded() {
        Spi::run("SET pg_otel.query_text = 'raw'").unwrap();
        let long = "x".repeat(10_000);
        let spans = traced_spans(&format!("SELECT '{long}'"));
        let text = query_text(query_span(&spans)).expect("query text");
        assert_eq!(text.len(), crate::span::QUERY_TEXT_MAX_LEN);
        assert!(
            text.len() <= crate::span::QUERY_TEXT_MAX_LEN,
            "{}",
            text.len()
        );
    }

    #[pg_test]
    fn query_id_is_exported_when_computed() {
        Spi::run("SET compute_query_id = on").unwrap();
        let spans = traced_spans("SELECT 1");
        assert_ne!(number(query_span(&spans), "db.query.id"), 0.0);

        Spi::run("SET compute_query_id = off").unwrap();
        let spans = traced_spans("SELECT 1");
        assert!(attr(query_span(&spans), "db.query.id").is_none());
    }

    #[pg_test]
    fn duration_is_in_plausible_microseconds() {
        let started = std::time::Instant::now();
        let spans = traced_spans("SELECT pg_sleep(0.05)");
        let measured_us = started.elapsed().as_micros() as f64;
        let span = query_span(&spans);
        let micros = number(span, "span.duration.us");
        assert!((40_000.0..5_000_000.0).contains(&micros), "{micros}us");
        // Raw TSC ticks mistaken for nanoseconds would exceed the real wall
        // time measured independently here. The slack absorbs TSC calibration
        // error and scheduling noise; a ticks-vs-ns mixup still fails by far.
        assert!(
            micros <= measured_us * 1.05 + 1_000.0,
            "{micros}us > measured {measured_us}us"
        );
        let seconds = number(span, "postgresql.execution.total_time_seconds");
        assert!((micros / 1e6 - seconds).abs() < 1e-9);
        let wall = span.end_time.duration_since(span.start_time).unwrap();
        assert_eq!(wall.as_micros() as f64, micros.floor());
    }

    #[pg_test]
    fn plan_node_reports_rows_and_loops() {
        Spi::run("CREATE TEMP TABLE otel_rows AS SELECT generate_series(1, 10) AS i").unwrap();
        let spans = traced_spans("SELECT * FROM otel_rows");
        let query = query_span(&spans);
        let scan = spans
            .iter()
            .find(|s| {
                attr(s, "postgresql.plan.node_type").is_some_and(|v| v.as_str() == "T_SeqScanState")
            })
            .expect("a seq scan span");
        assert_eq!(number(scan, "postgresql.instrumentation.rows"), 10.0);
        assert_eq!(number(scan, "postgresql.instrumentation.loops"), 1.0);
        assert!(number(scan, "span.duration.us") >= 0.0);
        assert_eq!(scan.parent_span_id, query.span_context.span_id());
        assert_eq!(scan.start_time, query.start_time);
        assert!(scan.end_time <= query.end_time);
    }

    #[pg_test]
    fn plan_node_reports_only_its_own_relation() {
        Spi::run("CREATE TEMP TABLE otel_a AS SELECT 1 AS i").unwrap();
        Spi::run("CREATE TEMP TABLE otel_b AS SELECT 1 AS i").unwrap();
        let spans = traced_spans("SELECT * FROM otel_a a JOIN otel_b b USING (i)");
        let mut relations: Vec<String> = spans
            .iter()
            .filter_map(|s| attr(s, "postgresql.plan.relation"))
            .map(|v| v.as_str().into_owned())
            .collect();
        relations.sort();
        assert_eq!(relations, ["pg_temp.otel_a", "pg_temp.otel_b"]);
        // Nodes that do not scan a relation carry none (no subtree lists).
        let joins = spans
            .iter()
            .filter(|s| {
                attr(s, "postgresql.plan.node_type")
                    .is_some_and(|v| v.as_str().ends_with("JoinState"))
            })
            .collect::<Vec<_>>();
        assert!(!joins.is_empty());
        assert!(
            joins
                .iter()
                .all(|s| attr(s, "postgresql.plan.relation").is_none())
        );
        // The query span still names every relation, computed once for the plan.
        assert_eq!(
            query_span(&spans).name,
            "SELECT pg_temp.otel_a, pg_temp.otel_b"
        );
    }

    // Note: this test empties the shared queue first, which discards spans that
    // concurrently running tests (or the worker's own backlog) queued; no other
    // test relies on queued spans reaching the worker.
    #[pg_test]
    fn span_round_trips_through_the_real_shared_memory_ring() {
        use crate::{codec, queue, shared};

        Spi::run("SET pg_otel.min_duration_ms = 0").unwrap();
        Spi::run("CREATE TEMP TABLE otel_ring AS SELECT generate_series(1, 3) AS i").unwrap();
        capture::start();
        Spi::run("SELECT * FROM otel_ring").unwrap();
        let spans = capture::finish();
        assert!(spans.len() >= 2);

        // Push and drain within one critical section: the background worker
        // (which drains the same queue) cannot interleave, so this is
        // deterministic. No Postgres calls inside the closure.
        let (batch, skipped) = codec::encode_batch(&spans);
        assert_eq!(skipped, 0);
        let mut drained = Vec::new();
        let records = shared::with_ring(|ring| {
            // Whatever the worker has not drained yet is not ours; set it
            // aside by draining first.
            let mut foreign = Vec::new();
            ring.drain_into(&mut foreign, usize::MAX).unwrap();
            ring.push_batch(&batch).expect("a fresh ring has room");
            ring.drain_into(&mut drained, usize::MAX).unwrap()
        })
        .expect("shared memory is set up");
        assert_eq!(records, spans.len());

        let (decoded, undecodable) = codec::decode_records(&drained);
        assert_eq!(undecodable, 0);
        assert_eq!(decoded, spans);
        assert!(queue::records(&drained).all(|r| r.is_ok()));
    }

    #[pg_test]
    fn full_queue_drops_the_whole_batch_and_counts_it() {
        use crate::{queue::Batch, shared};

        let before = shared::dropped_spans();
        // Far larger than any allowed queue (the maximum is 1 GiB, the default
        // 1 MiB), so this fails regardless of what is queued. Build it from
        // maximum-size records to stay within the per-record limit.
        let mut batch = Batch::new();
        let record_len = crate::queue::MAX_RECORD_BYTES;
        let queue_bytes = shared::with_ring(|ring| ring.capacity()).unwrap();
        while batch.len_bytes() <= queue_bytes {
            batch
                .push_record(|out| out.resize(out.len() + record_len, 0))
                .unwrap();
        }
        let count = batch.records();
        assert_eq!(shared::publish(&batch, 2), shared::Published::Dropped);
        // Other tests share the counter; it can only have grown further.
        assert!(shared::dropped_spans() >= before + count as u64 + 2);
    }

    #[pg_test]
    fn dropped_spans_are_visible_in_sql() {
        // The counter is shared with concurrently running tests, so bracket the
        // SQL reading between two snapshots instead of expecting equality.
        let before = crate::shared::dropped_spans();
        let from_sql = Spi::get_one::<i64>("SELECT pg_otel_dropped_spans()")
            .unwrap()
            .unwrap() as u64;
        let after = crate::shared::dropped_spans();
        assert!(
            (before..=after).contains(&from_sql),
            "{before} <= {from_sql} <= {after}"
        );
    }

    #[pg_test]
    fn real_queries_reach_the_queue_without_errors() {
        Spi::run("SET pg_otel.min_duration_ms = 0").unwrap();
        for _ in 0..20 {
            assert_eq!(query_value("SELECT 1"), Some(1));
        }
    }

    fn node_type(span: &SpanData) -> Option<String> {
        attr(span, "postgresql.plan.node_type").map(|v| v.as_str().into_owned())
    }

    fn text_attr(span: &SpanData, key: &str) -> Option<String> {
        attr(span, key).map(|v| v.as_str().into_owned())
    }

    fn plan_nodes(spans: &[SpanData]) -> Vec<&SpanData> {
        spans.iter().filter(|s| node_type(s).is_some()).collect()
    }

    fn nodes_of_type<'a>(spans: &'a [SpanData], node_type_name: &str) -> Vec<&'a SpanData> {
        spans
            .iter()
            .filter(|s| node_type(s).as_deref() == Some(node_type_name))
            .collect()
    }

    fn children_of<'a>(spans: &'a [SpanData], parent: &SpanData) -> Vec<&'a SpanData> {
        spans
            .iter()
            .filter(|s| s.parent_span_id == parent.span_context.span_id())
            .collect()
    }

    fn members_of<'a>(spans: &'a [SpanData], parent: &SpanData) -> Vec<&'a SpanData> {
        children_of(spans, parent)
            .into_iter()
            .filter(|s| {
                text_attr(s, "postgresql.plan.parent_relationship").as_deref() == Some("Member")
            })
            .collect()
    }

    fn relations(spans: &[&SpanData]) -> Vec<String> {
        let mut relations: Vec<String> = spans
            .iter()
            .filter_map(|s| text_attr(s, "postgresql.plan.relation"))
            .collect();
        relations.sort();
        relations
    }

    fn create_partitions() {
        Spi::run("CREATE TEMP TABLE otel_p (k int) PARTITION BY RANGE (k)").unwrap();
        Spi::run("CREATE TEMP TABLE otel_p1 PARTITION OF otel_p FOR VALUES FROM (0) TO (10)")
            .unwrap();
        Spi::run("CREATE TEMP TABLE otel_p2 PARTITION OF otel_p FOR VALUES FROM (10) TO (20)")
            .unwrap();
        Spi::run("CREATE TEMP TABLE otel_p3 PARTITION OF otel_p FOR VALUES FROM (20) TO (30)")
            .unwrap();
        Spi::run("INSERT INTO otel_p SELECT g FROM generate_series(0, 29) g").unwrap();
    }

    fn create_pair() {
        Spi::run("CREATE TEMP TABLE otel_a AS SELECT 1 AS i").unwrap();
        Spi::run("CREATE TEMP TABLE otel_b AS SELECT 1 AS i").unwrap();
    }

    #[pg_test]
    fn append_reports_every_partition_as_a_member() {
        create_partitions();
        let spans = traced_spans("SELECT * FROM otel_p");
        let appends = nodes_of_type(&spans, "T_AppendState");
        assert_eq!(appends.len(), 1);
        let members = members_of(&spans, appends[0]);
        assert_eq!(
            relations(&members),
            ["pg_temp.otel_p1", "pg_temp.otel_p2", "pg_temp.otel_p3"]
        );
        // Nothing was pruned.
        assert!(attr(appends[0], "postgresql.plan.subplans_removed").is_none());
        // The member scans are not attached to the query span directly.
        assert!(
            members
                .iter()
                .all(|m| m.parent_span_id == appends[0].span_context.span_id())
        );
    }

    #[pg_test]
    fn runtime_pruning_reports_only_live_members_and_the_removed_count() {
        create_partitions();
        Spi::run("PREPARE otel_q(int) AS SELECT * FROM otel_p WHERE k = $1").unwrap();
        Spi::run("SET plan_cache_mode = force_generic_plan").unwrap();
        let spans = traced_spans("EXECUTE otel_q(5)");
        let appends = nodes_of_type(&spans, "T_AppendState");
        assert_eq!(
            appends.len(),
            1,
            "{:?}",
            plan_nodes(&spans)
                .iter()
                .map(|s| &s.name)
                .collect::<Vec<_>>()
        );
        let members = members_of(&spans, appends[0]);
        assert_eq!(relations(&members), ["pg_temp.otel_p1"]);
        assert_eq!(number(appends[0], "postgresql.plan.subplans_removed"), 2.0);
    }

    #[pg_test]
    fn union_all_members_are_children_of_the_append() {
        create_pair();
        let spans = traced_spans("SELECT i FROM otel_a UNION ALL SELECT i FROM otel_b");
        let appends = nodes_of_type(&spans, "T_AppendState");
        assert_eq!(appends.len(), 1);
        assert_eq!(
            relations(&members_of(&spans, appends[0])),
            ["pg_temp.otel_a", "pg_temp.otel_b"]
        );
    }

    #[pg_test]
    fn bitmap_or_members_are_the_index_scans() {
        Spi::run(
            "CREATE TEMP TABLE otel_bm AS SELECT g AS a, g AS b FROM generate_series(1, 10000) g",
        )
        .unwrap();
        Spi::run("CREATE INDEX otel_bm_a ON otel_bm (a)").unwrap();
        Spi::run("CREATE INDEX otel_bm_b ON otel_bm (b)").unwrap();
        Spi::run("ANALYZE otel_bm").unwrap();
        Spi::run("SET enable_seqscan = off").unwrap();
        Spi::run("SET enable_indexscan = off").unwrap();
        let spans = traced_spans("SELECT * FROM otel_bm WHERE a = 1 OR b = 2");
        let ors = nodes_of_type(&spans, "T_BitmapOrState");
        assert_eq!(ors.len(), 1);
        let members = members_of(&spans, ors[0]);
        assert_eq!(members.len(), 2);
        assert!(
            members
                .iter()
                .all(|m| node_type(m).as_deref() == Some("T_BitmapIndexScanState"))
        );
    }

    #[pg_test]
    fn correlated_subplan_hangs_off_the_node_that_evaluates_it() {
        create_pair();
        let spans = traced_spans(
            "SELECT * FROM otel_a a WHERE a.i = (SELECT max(b.i) FROM otel_b b WHERE b.i = a.i)",
        );
        let subplans: Vec<_> = spans
            .iter()
            .filter(|s| {
                text_attr(s, "postgresql.plan.parent_relationship").as_deref() == Some("SubPlan")
            })
            .collect();
        assert_eq!(subplans.len(), 1);
        let name = text_attr(subplans[0], "postgresql.plan.subplan_name").unwrap();
        assert!(name.starts_with("SubPlan"), "{name}");
        assert!(
            subplans[0].name.starts_with(&format!("{name} → ")),
            "{}",
            subplans[0].name
        );
        let parent = spans
            .iter()
            .find(|s| s.span_context.span_id() == subplans[0].parent_span_id)
            .expect("the sub-plan's parent has a span");
        assert_eq!(
            text_attr(parent, "postgresql.plan.relation").as_deref(),
            Some("pg_temp.otel_a")
        );
    }

    #[pg_test]
    fn uncorrelated_subquery_is_an_init_plan_of_the_top_node() {
        create_pair();
        let spans = traced_spans("SELECT * FROM otel_a WHERE i = (SELECT max(i) FROM otel_b)");
        let init_plans: Vec<_> = spans
            .iter()
            .filter(|s| {
                text_attr(s, "postgresql.plan.parent_relationship").as_deref() == Some("InitPlan")
            })
            .collect();
        assert_eq!(init_plans.len(), 1);
        let name = text_attr(init_plans[0], "postgresql.plan.subplan_name").unwrap();
        assert!(name.starts_with("InitPlan"), "{name}");
        let query = query_span(&spans);
        let top = spans
            .iter()
            .find(|s| s.span_context.span_id() == init_plans[0].parent_span_id)
            .expect("the init plan's parent has a span");
        assert_eq!(top.parent_span_id, query.span_context.span_id());
    }

    #[pg_test]
    fn cte_is_reported_as_a_named_sub_plan() {
        create_pair();
        let spans = traced_spans("WITH c AS MATERIALIZED (SELECT i FROM otel_a) SELECT * FROM c");
        let names: Vec<_> = spans
            .iter()
            .filter_map(|s| text_attr(s, "postgresql.plan.subplan_name"))
            .collect();
        assert_eq!(names, ["CTE c"]);
        assert_eq!(nodes_of_type(&spans, "T_CteScanState").len(), 1);
    }

    #[pg_test]
    fn subquery_scan_child_has_the_subquery_relationship() {
        create_pair();
        let spans = traced_spans("SELECT * FROM (SELECT i FROM otel_a OFFSET 0) s WHERE s.i > 0");
        let scans = nodes_of_type(&spans, "T_SubqueryScanState");
        assert_eq!(scans.len(), 1);
        let children = children_of(&spans, scans[0]);
        assert_eq!(children.len(), 1);
        assert_eq!(
            text_attr(children[0], "postgresql.plan.parent_relationship").as_deref(),
            Some("Subquery")
        );
        assert_eq!(
            text_attr(children[0], "postgresql.plan.relation").as_deref(),
            Some("pg_temp.otel_a")
        );
    }

    #[pg_test]
    fn never_executed_nodes_are_flagged_with_zero_numbers() {
        create_pair();
        let spans = traced_spans("SELECT * FROM otel_a LIMIT 0");
        let never: Vec<_> = spans
            .iter()
            .filter(|s| attr(s, "postgresql.plan.never_executed").is_some())
            .collect();
        assert_eq!(
            never.len(),
            1,
            "{:?}",
            plan_nodes(&spans)
                .iter()
                .map(|s| &s.name)
                .collect::<Vec<_>>()
        );
        assert_eq!(node_type(never[0]).as_deref(), Some("T_SeqScanState"));
        assert_eq!(number(never[0], "postgresql.instrumentation.loops"), 0.0);
        assert_eq!(number(never[0], "postgresql.instrumentation.rows"), 0.0);
        assert_eq!(number(never[0], "span.duration.us"), 0.0);
        // Nodes that ran carry no such flag.
        let executed = nodes_of_type(&spans, "T_LimitState");
        assert!(attr(executed[0], "postgresql.plan.never_executed").is_none());
    }

    #[pg_test]
    fn plan_span_cap_truncates_and_reports_it_on_the_query_span() {
        create_partitions();
        Spi::run("SET pg_otel.max_plan_spans = 2").unwrap();
        let spans = traced_spans("SELECT * FROM otel_p");
        // Append and three scans: two plan spans are kept, two are omitted.
        assert_eq!(plan_nodes(&spans).len(), 2);
        let query = query_span(&spans);
        assert!(matches!(
            attr(query, "postgresql.plan.spans_truncated"),
            Some(Value::Bool(true))
        ));
        assert_eq!(number(query, "postgresql.plan.spans_omitted"), 2.0);
        // The kept part is connected: top node under the query, one member under it.
        let top = nodes_of_type(&spans, "T_AppendState");
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].parent_span_id, query.span_context.span_id());
        assert_eq!(members_of(&spans, top[0]).len(), 1);

        Spi::run("SET pg_otel.max_plan_spans = 0").unwrap();
        let spans = traced_spans("SELECT * FROM otel_p");
        assert!(plan_nodes(&spans).is_empty());
        assert_eq!(
            number(query_span(&spans), "postgresql.plan.spans_omitted"),
            4.0
        );
    }

    #[pg_test]
    fn complete_plans_carry_no_truncation_marker() {
        create_partitions();
        let spans = traced_spans("SELECT * FROM otel_p");
        assert!(attr(query_span(&spans), "postgresql.plan.spans_truncated").is_none());
        assert!(attr(query_span(&spans), "postgresql.plan.spans_omitted").is_none());
    }

    #[pg_test]
    fn deeply_nested_plans_are_collected() {
        create_pair();
        // Explicit join order (no reordering) makes a plan that is 40 joins deep.
        Spi::run("SET join_collapse_limit = 1").unwrap();
        let mut sql = String::from("SELECT 1 FROM otel_a t0");
        for n in 1..40 {
            sql.push_str(&format!(" JOIN otel_a t{n} ON t{n}.i = t{}.i", n - 1));
        }
        let spans = traced_spans(&sql);
        let scans = nodes_of_type(&spans, "T_SeqScanState").len();
        assert_eq!(scans, 40);
        assert!(
            plan_nodes(&spans).len() >= 79,
            "{}",
            plan_nodes(&spans).len()
        );
        assert!(attr(query_span(&spans), "postgresql.plan.spans_truncated").is_none());
    }

    /// pg_otel is preloaded before pg_stat_statements (see `pg_test::postgresql_conf_options`),
    /// the order in which a statement timer allocated without buffer/WAL usage
    /// would starve pg_stat_statements.
    #[pg_test]
    fn pg_stat_statements_still_sees_buffer_usage() {
        Spi::run("CREATE EXTENSION pg_stat_statements").unwrap();
        Spi::run("CREATE TABLE otel_pgss_t AS SELECT g AS i FROM generate_series(1, 1000) g")
            .unwrap();
        Spi::run("SET pg_otel.min_duration_ms = 0").unwrap();
        // The test function itself is a top-level statement, so the statement
        // under test is nested and only tracked with `track = all`.
        Spi::run("SET pg_stat_statements.track = 'all'").unwrap();
        Spi::run("SELECT pg_stat_statements_reset()").unwrap();

        Spi::run("SELECT count(*) FROM otel_pgss_t").unwrap();

        let blocks = Spi::get_one::<i64>(
            "SELECT sum(shared_blks_hit + shared_blks_read)::bigint FROM pg_stat_statements \
             WHERE query LIKE 'SELECT count(*) FROM otel_pgss_t%'",
        )
        .unwrap()
        .expect("pg_stat_statements recorded the statement");
        assert!(blocks > 0, "no buffer usage recorded: {blocks}");
    }

    #[pg_test]
    fn explain_without_analyze_emits_nothing() {
        let spans = traced_spans("EXPLAIN SELECT 1");
        assert!(spans.is_empty(), "{} unexpected spans", spans.len());
    }

    #[pg_test]
    fn explain_analyze_still_emits_spans() {
        let spans = traced_spans("EXPLAIN (ANALYZE) SELECT 1");
        assert!(!spans.is_empty());
    }

    fn query_value(sql: &str) -> Option<i32> {
        Spi::get_one::<i32>(sql).expect("query must not fail")
    }

    #[pg_test]
    fn queries_succeed_with_tracing_disabled() {
        Spi::run("SET pg_otel.min_duration_ms = -1").unwrap();
        assert_eq!(query_value("SELECT 1"), Some(1));
    }

    #[pg_test]
    fn queries_succeed_with_tracing_enabled() {
        Spi::run("SET pg_otel.min_duration_ms = 0").unwrap();
        assert_eq!(query_value("SELECT 1 /* a */ /* b */"), Some(1));
        assert_eq!(query_value("SELECT 2 /*pg_otel.traceparent=X*/"), Some(2));
        assert_eq!(
            query_value("SELECT 3 /* é */ /* traceparent='é' */"),
            Some(3)
        );
        assert_eq!(
            query_value(&format!("SELECT 4 /*traceparent='{TP}'*/")),
            Some(4)
        );
    }

    #[pg_test]
    fn queries_succeed_with_traceparent_guc() {
        Spi::run("SET pg_otel.min_duration_ms = 0").unwrap();
        for value in [TP, TP_UNSAMPLED, "garbage", ""] {
            Spi::run(&format!("SET LOCAL pg_otel.traceparent = '{value}'")).unwrap();
            assert_eq!(query_value("SELECT 1"), Some(1));
        }
    }

    fn assert_next_query_works() {
        assert_eq!(query_value("SELECT 42"), Some(42));
        // A second statement proves the span queue lock was not leaked.
        assert_eq!(query_value("SELECT 43"), Some(43));
    }

    #[pg_test]
    fn postgres_error_in_collection_does_not_fail_query() {
        Spi::run("SET pg_otel.min_duration_ms = 0").unwrap();
        fault::arm(Fault::PostgresError);
        assert_eq!(query_value("SELECT 1"), Some(1));
        assert_next_query_works();
    }

    #[pg_test]
    fn panic_in_collection_does_not_fail_query() {
        Spi::run("SET pg_otel.min_duration_ms = 0").unwrap();
        fault::arm(Fault::Panic);
        assert_eq!(query_value("SELECT 1"), Some(1));
        assert_next_query_works();
    }

    #[pg_test(error = "injected query cancel")]
    fn query_cancel_in_collection_is_not_swallowed() {
        Spi::run("SET pg_otel.min_duration_ms = 0").unwrap();
        fault::arm(Fault::QueryCancel);
        let _ = Spi::run("SELECT 1");
    }

    #[pg_test]
    fn instrumentation_is_not_requested_when_disabled() {
        Spi::run("SET pg_otel.min_duration_ms = -1").unwrap();
        // A statement started while tracing was disabled has no query
        // instrumentation; enabling it before the statement ends must not crash.
        Spi::run("SET pg_otel.min_duration_ms = 0").unwrap();
        assert_eq!(query_value("SELECT 1"), Some(1));
    }
}
