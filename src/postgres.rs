use std::{
    ffi::CStr,
    panic::AssertUnwindSafe,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, SystemTime},
};

use opentelemetry::trace::SpanContext;
use pgrx::{
    PgSqlErrorCode, PgTryBuilder, debug1, log,
    pg_sys::{
        self,
        InstrumentOption::{INSTRUMENT_ROWS, INSTRUMENT_TIMER},
        Oid,
    },
};

use crate::{
    DEQUE, WORKER_PID,
    config::{get_min_duration_ms, get_otlp_traceparent},
    span::{HeaplessSpan, ParentContext, parse_traceparent},
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

/// Asks the executor to collect the per-node instrumentation needed for spans.
///
/// Does nothing when tracing is disabled, so untraced statements pay no
/// instrumentation overhead.
pub fn request_instrumentation(query_desc: *mut pg_sys::QueryDesc) {
    if query_desc.is_null() || !tracing_enabled_here() {
        return;
    }

    // SAFETY: We check that `query_desc` is not null before dereferencing it.
    unsafe {
        (*query_desc).query_instr_options |= (INSTRUMENT_ROWS | INSTRUMENT_TIMER) as i32;
        (*query_desc).instrument_options |= (INSTRUMENT_ROWS | INSTRUMENT_TIMER) as i32;
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

    // SAFETY: `query_desc` is non-null and valid for the duration of the hook.
    // `query_instr` is null when instrumentation was not requested (tracing
    // was disabled at ExecutorStart).
    let Some(query_instr) = (unsafe { (*query_desc).query_instr.as_ref() }) else {
        return;
    };

    // Use the current time to calculate the duration of the query.
    // This should be close enough to the actual end time.
    let end_time = SystemTime::now();
    let total = query_instr.total.ticks;
    if !meets_slow_query_threshold(total, get_min_duration_ms()) {
        return;
    }
    let wall_start = end_time - Duration::from_nanos(total as u64);

    let source_text = pg_str(unsafe { (*query_desc).sourceText });
    let guc_traceparent = get_otlp_traceparent();
    let parent = match decide_parent(guc_traceparent.as_deref(), source_text) {
        TraceDecision::Skip => return,
        TraceDecision::Trace(parent) => parent,
    };
    let Some(span) = HeaplessSpan::from_query(query_desc, wall_start, &parent) else {
        return;
    };

    let planstate = unsafe { (*query_desc).planstate };
    collect_plan_spans(planstate, wall_start, &span);
    // Never call Postgres code while holding the DEQUE guard: if that code
    // raised an ERROR that we catch, the LWLock would leak (pgrx only releases
    // it on unwind when InterruptHoldoffCount is non-zero), and every later
    // enqueue would hang. Build the span first, lock only to push it.
    let _ = DEQUE.exclusive().enqueue(span);
    pg_otel_wake_worker();
}

/// `-1` disables tracing, `0` traces every statement.
fn meets_slow_query_threshold(duration_ns: i64, min_duration_ms: i32) -> bool {
    min_duration_ms >= 0 && duration_ns >= i64::from(min_duration_ms) * 1_000_000
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
    // Same rule as in `collect_spans_unguarded`: no Postgres calls while the
    // DEQUE guard is held (a caught ERROR would leak the LWLock).
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
    if s.is_null() {
        return None;
    }
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
    use pgrx::prelude::*;

    use super::fault::{self, Fault};

    const TP: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    const TP_UNSAMPLED: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";

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
