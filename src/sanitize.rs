//! Sanitization of SQL text before it is exported in traces.
//!
//! The pipeline is always **extract → normalize → truncate**:
//!
//! 1. [`current_statement`] cuts the statement of interest out of a
//!    multi-statement query string, so text of sibling statements is never
//!    exported.
//! 2. [`normalize`] re-lexes that statement with Postgres' own core scanner and
//!    replaces every literal constant with a numbered placeholder (`$1`, `$2`,
//!    ...), dropping comments and collapsing whitespace.
//! 3. [`truncate_utf8`] caps the result at a byte budget on a character
//!    boundary.
//!
//! Truncation comes last on purpose: truncating first could cut a literal in
//! half and make the scanner fail (or leave a literal prefix unmasked).
//! Normalizing first guarantees that only already-masked text is truncated.
//!
//! [`sanitize`] ties the three steps together.
//!
//! # Guarantees and limits
//!
//! * Normalization fails closed: input that cannot be normalized (scanner
//!   error, NUL byte, more than [`MAX_NORMALIZE_INPUT_BYTES`] bytes, or a
//!   server encoding other than UTF-8) yields `None`, never the raw text. The
//!   scanner interprets bytes in the server encoding while Rust strings are
//!   UTF-8, so other encodings are rejected rather than converted
//!   (`pg_do_encoding_conversion` can itself raise errors and needs a
//!   transaction).
//! * Databases using the `SQL_ASCII` encoding (or any other non-UTF-8 server
//!   encoding) therefore get **no query text at all** in normalized mode.
//! * `raw` query text mode bypasses normalization, the input size cap and the
//!   encoding check by design: it exports the statement as the server received
//!   it (lossily converted to UTF-8). Only the final truncation applies.
//! * Only literal *values* are masked. **Quoted identifiers (`"..."`) are kept
//!   verbatim**, as are identifiers, keywords and operators.
//! * Comments are removed, so text hidden in comments never leaves the server.
//! * The scanner emits `NOTICE`s for over-long identifiers; those are
//!   suppressed during the scan so exporting a span never adds client or log
//!   messages.
//!
//! # Token handling
//!
//! The scanner skips whitespace and comments and returns one token at a time
//! together with its start offset. Flex NUL-terminates the current token inside
//! the scanner's private copy of the input, so the token end is found with a
//! `strlen` from the start offset (this is the same technique used by
//! Postgres' own query jumbling code). Text between two tokens is therefore
//! never copied: any non-empty gap (whitespace and/or comments) becomes a
//! single space, which keeps `a/**/b` as two tokens.
//!
//! Constants (`SCONST`, `USCONST`, `ICONST`, `FCONST`, `BCONST`, `XCONST`) are
//! replaced by placeholders. Because the lexer does not know the grammar, a
//! unary minus is a separate operator token and stays in the output:
//! `x = -5` becomes `x = -$1`. Existing `$n` parameters are kept and
//! new placeholders are numbered above the highest existing one so they never
//! collide. A space is inserted where a placeholder would otherwise fuse with a
//! neighbouring word (`N'x'` becomes `N $1`, not `N$1`).

// Wired into span export in a later phase.
#![allow(dead_code)]

use std::{
    ffi::{CStr, CString, c_char, c_int},
    fmt::Write as _,
};

use pgrx::{PgMemoryContexts, PgTryBuilder, pg_sys};

// Token codes of the core scanner. They are not part of the pgrx bindings
// because they live in the bison-generated `gram.h`. `parser/scanner.h`
// guarantees these come first in every grammar built on the core scanner
// (`IDENT = 258` and so on), so the numbers are stable across Postgres versions;
// the values below were taken from the PG19 `gram.h`.
const FCONST: c_int = 260;
const SCONST: c_int = 261;
const USCONST: c_int = 262;
const BCONST: c_int = 263;
const XCONST: c_int = 264;
const ICONST: c_int = 266;
const PARAM: c_int = 267;

/// Scanner's end-of-input token code.
const END_OF_INPUT: c_int = 0;

/// Largest input, in bytes, that [`normalize`] accepts. Bigger statements are
/// rejected (fail closed) to bound the time and memory spent in the executor
/// hook. Because normalization runs before truncation, a statement above this
/// size produces no text at all even when `max_len` is small.
pub const MAX_NORMALIZE_INPUT_BYTES: usize = 64 * 1024;

/// What the normalizer needs to know about a token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenKind {
    /// A literal value that must be masked.
    Constant,
    /// An existing `$n` parameter reference carrying its number.
    Param(c_int),
    /// Anything else (keywords, identifiers, operators, punctuation).
    Other,
}

impl TokenKind {
    fn from_code(code: c_int, ival: impl FnOnce() -> c_int) -> Self {
        match code {
            FCONST | SCONST | USCONST | BCONST | XCONST | ICONST => Self::Constant,
            PARAM => Self::Param(ival()),
            _ => Self::Other,
        }
    }
}

/// A token as a byte range of the scanned text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Token {
    kind: TokenKind,
    start: usize,
    end: usize,
}

/// Extracts the statement at `stmt_location`/`stmt_len` from `source_text`.
///
/// `stmt_location` and `stmt_len` follow the `PlannedStmt` conventions: a
/// negative location means "unknown, use the whole string" and a length of
/// zero (or less) means "up to the end of the string". Leading and trailing
/// whitespace is removed using Postgres' `CleanQuerytext`. Invalid UTF-8 is
/// replaced lossily.
///
/// Returns `None` when `source_text` is NULL, the location lies outside the
/// string, or the statement is empty.
///
/// # Safety
///
/// `source_text` must be NULL or point to a NUL-terminated string that stays
/// valid for the duration of the call.
pub unsafe fn current_statement(
    source_text: *const c_char,
    stmt_location: i32,
    stmt_len: i32,
) -> Option<String> {
    if source_text.is_null() {
        return None;
    }
    // SAFETY: non-null and NUL-terminated per the function contract.
    let bytes = unsafe { CStr::from_ptr(source_text) }.to_bytes();
    let (mut location, mut len) = validated_range(bytes.len(), stmt_location, stmt_len)?;

    // CleanQuerytext only trims; it needs `location <= strlen` and
    // `len <= strlen - location`, which `validated_range` guarantees.
    // SAFETY: the pointer is valid per the function contract.
    let _ = unsafe { pg_sys::CleanQuerytext(source_text, &mut location, &mut len) };

    let range = location as usize..(location + len) as usize;
    let statement = String::from_utf8_lossy(bytes.get(range)?);
    (!statement.is_empty()).then(|| statement.into_owned())
}

/// Returns `(location, len)` that are safe to pass to `CleanQuerytext` for a
/// string of `total` bytes, or `None` if `stmt_location` is out of range.
/// An over-long `stmt_len` is clamped to the rest of the string.
fn validated_range(total: usize, stmt_location: i32, stmt_len: i32) -> Option<(i32, i32)> {
    let total = i32::try_from(total).ok()?;
    if stmt_location < 0 {
        return Some((-1, 0));
    }
    if stmt_location > total {
        return None;
    }
    let rest = total - stmt_location;
    let len = if stmt_len > rest { 0 } else { stmt_len };
    Some((stmt_location, len))
}

/// Replaces literal constants in `sql` with `$1`, `$2`, ... and drops comments.
///
/// See the [module documentation](self) for the exact output rules. The
/// function never raises a Postgres error and emits no notices: input that the
/// scanner rejects (for example an unterminated string) yields `None`, and so
/// does input containing a NUL byte, input larger than
/// [`MAX_NORMALIZE_INPUT_BYTES`], and any database whose encoding is not
/// UTF-8. Callers must not fall back to the raw text in that case.
pub fn normalize(sql: &str) -> Option<String> {
    if sql.len() > MAX_NORMALIZE_INPUT_BYTES || !server_encoding_is_utf8() {
        return None;
    }
    let sql_c = CString::new(sql).ok()?;
    let tokens = lex(&sql_c)?;
    render(sql, &tokens)
}

fn server_encoding_is_utf8() -> bool {
    // SAFETY: plain getter of a backend-local variable.
    unsafe { pg_sys::GetDatabaseEncoding() == pg_sys::pg_enc::PG_UTF8 as c_int }
}

/// Temporarily raises `client_min_messages` and the log level of this backend
/// type to `ERROR`, so notices raised by the scanner (over-long identifier
/// truncation) are neither sent to the client nor logged. Levels already above
/// `ERROR` are left alone. The previous values are restored on drop, which
/// also covers unwinding.
///
/// The backing C variables are written directly instead of going through
/// `SetConfigOption`/`set_config_option`: that path runs GUC assign hooks,
/// records the change in the current transaction's GUC stack (needing an active
/// transaction and a matching nesting level, which is not guaranteed inside an
/// executor hook), and can itself `ereport`. Writing the variables is
/// side-effect free and cannot fail. The trade-off is that the GUC machinery
/// does not know about the change, so it must always be reverted before control
/// returns to Postgres, which `Drop` guarantees.
struct MessageSuppression {
    client_min_messages: c_int,
    log_min_messages: c_int,
}

impl MessageSuppression {
    const LEVEL: c_int = pgrx::PgLogLevel::ERROR as c_int;

    fn new() -> Self {
        // SAFETY: backend-local GUC variables, only touched by this backend.
        unsafe {
            let saved = Self {
                client_min_messages: pg_sys::client_min_messages,
                log_min_messages: Self::log_min_messages(),
            };
            pg_sys::client_min_messages = saved.client_min_messages.max(Self::LEVEL);
            Self::set_log_min_messages(saved.log_min_messages.max(Self::LEVEL));
            saved
        }
    }

    // PG19 keeps one `log_min_messages` level per backend type: the C variable
    // is an array declared as `extern int log_min_messages[];` in
    // `utils/guc.h`, which bindgen exposes as a zero-length array. Indexing it
    // directly would be out of bounds for the Rust type, so the slot is reached
    // through raw pointer arithmetic on the array's address. The bounds check
    // against `B_LOGGER` (the last backend type) keeps the offset inside the
    // real array.
    #[cfg(feature = "pg19")]
    unsafe fn log_min_messages_slot() -> Option<*mut c_int> {
        let backend_type = unsafe { pg_sys::MyBackendType } as usize;
        (backend_type <= pg_sys::BackendType::B_LOGGER as usize).then(|| unsafe {
            (&raw mut pg_sys::log_min_messages)
                .cast::<c_int>()
                .add(backend_type)
        })
    }

    #[cfg(not(feature = "pg19"))]
    unsafe fn log_min_messages_slot() -> Option<*mut c_int> {
        Some(&raw mut pg_sys::log_min_messages)
    }

    unsafe fn log_min_messages() -> c_int {
        // SAFETY: the slot points into Postgres' GUC variable.
        unsafe { Self::log_min_messages_slot().map_or(Self::LEVEL, |slot| *slot) }
    }

    unsafe fn set_log_min_messages(level: c_int) {
        // SAFETY: as above; we only write values read from the same slot or
        // `ERROR`.
        if let Some(slot) = unsafe { Self::log_min_messages_slot() } {
            unsafe { *slot = level };
        }
    }
}

impl Drop for MessageSuppression {
    fn drop(&mut self) {
        // SAFETY: restores the values captured in `new`.
        unsafe {
            pg_sys::client_min_messages = self.client_min_messages;
            Self::set_log_min_messages(self.log_min_messages);
        }
    }
}

/// Runs the core scanner over `sql`, returning `None` if it raises an error.
///
/// The scan happens in a throw-away memory context so the scanner's buffers
/// are released whether or not it fails, and with notices suppressed.
fn lex(sql: &CStr) -> Option<Vec<Token>> {
    let _quiet = MessageSuppression::new();
    let mut context = PgMemoryContexts::new("pg_otel sanitize");
    // SAFETY: the context is owned by this function and outlives the closure;
    // nothing allocated inside is used after it returns (tokens are plain Rust
    // data).
    unsafe {
        context.switch_to(|_| {
            PgTryBuilder::new(|| scan_tokens(sql))
                .catch_others(|_| None)
                .execute()
        })
    }
}

/// Collects all tokens of `sql`. May raise a Postgres error, so it must run
/// inside [`PgTryBuilder`]. Returns `None` if the scanner reports a token
/// position that does not fit a `usize`.
fn scan_tokens(sql: &CStr) -> Option<Vec<Token>> {
    let mut extra = pg_sys::core_yy_extra_type::default();
    let mut yylval = pg_sys::core_YYSTYPE::default();
    let mut yylloc: c_int = 0;
    let mut tokens = Vec::new();
    let mut valid = true;

    // SAFETY: `sql` is NUL-terminated; `extra` outlives the scanner and is
    // only used through it. The keyword tables are Postgres' own statics, as
    // required by `scanner_init`.
    let scanner = unsafe {
        pg_sys::scanner_init(
            sql.as_ptr(),
            &mut extra,
            &raw const pg_sys::ScanKeywords,
            pg_sys::ScanKeywordTokens.as_ptr(),
        )
    };
    loop {
        // SAFETY: `scanner` was returned by `scanner_init` and is not finished.
        let code = unsafe { pg_sys::core_yylex(&mut yylval, &mut yylloc, scanner) };
        if code == END_OF_INPUT {
            break;
        }
        // SAFETY: only read for PARAM tokens, where the scanner set `ival`.
        let kind = TokenKind::from_code(code, || unsafe { yylval.ival });
        let Ok(start) = usize::try_from(yylloc) else {
            valid = false;
            break;
        };
        // SAFETY: flex terminated the current token with a NUL inside
        // `scanbuf`, which is itself NUL-terminated, so this stays in bounds.
        let len = unsafe { CStr::from_ptr(extra.scanbuf.add(start)) }.count_bytes();
        tokens.push(Token {
            kind,
            start,
            end: start + len,
        });
    }
    // SAFETY: matches the `scanner_init` above.
    unsafe { pg_sys::scanner_finish(scanner) };
    valid.then_some(tokens)
}

/// Builds the normalized text from `sql` and its tokens (in source order).
///
/// Returns `None` if the tokens are inconsistent with `sql` (out of order, out
/// of bounds, or not on character boundaries).
fn render(sql: &str, tokens: &[Token]) -> Option<String> {
    let mut next_placeholder = tokens
        .iter()
        .filter_map(|token| match token.kind {
            TokenKind::Param(number) => Some(number),
            _ => None,
        })
        .max()
        .map_or(1, |highest| highest.saturating_add(1));

    let mut out = String::with_capacity(sql.len());
    let mut previous_end: Option<usize> = None;
    let mut placeholder = String::new();
    for token in tokens {
        let text = match token.kind {
            TokenKind::Constant => {
                placeholder.clear();
                // Writing to a String cannot fail.
                let _ = write!(placeholder, "${next_placeholder}");
                next_placeholder = next_placeholder.saturating_add(1);
                placeholder.as_str()
            }
            TokenKind::Param(_) | TokenKind::Other => sql.get(token.start..token.end)?,
        };
        let gap = match previous_end {
            Some(end) if token.start < end => return None,
            Some(end) => token.start > end,
            None => false,
        };
        if (gap || would_fuse(&out, text)) && !out.is_empty() {
            out.push(' ');
        }
        out.push_str(text);
        previous_end = Some(token.end);
    }
    Some(out)
}

/// Whether appending `next` to `out` without a separator would merge two
/// words when the result is lexed again (`N` + `$1` reads as `N$1`).
fn would_fuse(out: &str, next: &str) -> bool {
    let is_word = |c: char| c.is_alphanumeric() || c == '_' || c == '$' || !c.is_ascii();
    matches!(
        (out.chars().next_back(), next.chars().next()),
        (Some(last), Some(first)) if is_word(last) && is_word(first)
    )
}

/// Truncates `s` to at most `max_bytes` bytes, never splitting a UTF-8
/// character.
pub fn truncate_utf8(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Produces the query text to export: extract the current statement, then
/// optionally normalize it, then truncate to `max_len` bytes.
///
/// With `normalize_text` the result never contains literal values; if the
/// statement cannot be normalized, `None` is returned rather than leaking the
/// raw text. With `normalize_text == false` the raw statement is exported
/// (the caller has opted in to that). Callers implementing an "off" mode
/// should simply not call this function.
///
/// # Safety
///
/// Same contract as [`current_statement`].
pub unsafe fn sanitize(
    source_text: *const c_char,
    stmt_location: i32,
    stmt_len: i32,
    normalize_text: bool,
    max_len: usize,
) -> Option<String> {
    // SAFETY: forwarded contract.
    let statement = unsafe { current_statement(source_text, stmt_location, stmt_len) }?;
    let text = if normalize_text {
        normalize(&statement)?
    } else {
        statement
    };
    Some(truncate_utf8(&text, max_len).to_owned())
}

#[cfg(test)]
mod pure_tests {
    use super::*;

    fn token(kind: TokenKind, start: usize, end: usize) -> Token {
        Token { kind, start, end }
    }

    #[test]
    fn truncates_on_utf8_boundary() {
        assert_eq!(truncate_utf8("abcdef", 3), "abc");
        assert_eq!(truncate_utf8("abc", 3), "abc");
        assert_eq!(truncate_utf8("abc", 10), "abc");
        assert_eq!(truncate_utf8("abc", 0), "");
        // "é" is two bytes: a cut in its middle backs off to before it.
        assert_eq!(truncate_utf8("aé", 2), "a");
        assert_eq!(truncate_utf8("aé", 3), "aé");
        assert_eq!(truncate_utf8("日本語", 4), "日");
        assert_eq!(truncate_utf8("日本語", 2), "");
    }

    #[test]
    fn validated_range_handles_boundaries() {
        // Unknown location: whole string, length distrusted.
        assert_eq!(validated_range(10, -1, 5), Some((-1, 0)));
        // Exactly the rest of the string is kept as is.
        assert_eq!(validated_range(10, 4, 6), Some((4, 6)));
        // One byte too long is clamped to "rest of string".
        assert_eq!(validated_range(10, 4, 7), Some((4, 0)));
        assert_eq!(validated_range(10, 4, i32::MAX), Some((4, 0)));
        // Zero and negative lengths mean "rest of string" for CleanQuerytext.
        assert_eq!(validated_range(10, 4, 0), Some((4, 0)));
        assert_eq!(validated_range(10, 4, -3), Some((4, -3)));
        // Location at the very end is valid, past the end is not.
        assert_eq!(validated_range(10, 10, 0), Some((10, 0)));
        assert_eq!(validated_range(10, 11, 0), None);
        assert_eq!(validated_range(0, 0, 0), Some((0, 0)));
    }

    #[test]
    fn render_replaces_constants_in_order() {
        let sql = "a 1 , 'x'";
        let tokens = [
            token(TokenKind::Other, 0, 1),
            token(TokenKind::Constant, 2, 3),
            token(TokenKind::Other, 4, 5),
            token(TokenKind::Constant, 6, 9),
        ];
        assert_eq!(render(sql, &tokens).as_deref(), Some("a $1 , $2"));
    }

    #[test]
    fn render_numbers_above_existing_parameters() {
        let sql = "$7 'x'";
        let tokens = [
            token(TokenKind::Param(7), 0, 2),
            token(TokenKind::Constant, 3, 6),
        ];
        assert_eq!(render(sql, &tokens).as_deref(), Some("$7 $8"));
    }

    #[test]
    fn render_separates_words_that_would_fuse() {
        // `N` directly followed by a literal must not become `N$1`.
        let sql = "N'x'";
        let tokens = [
            token(TokenKind::Other, 0, 1),
            token(TokenKind::Constant, 1, 4),
        ];
        assert_eq!(render(sql, &tokens).as_deref(), Some("N $1"));
        // A placeholder followed by a word must not become `$1b`.
        let sql = "'x'b";
        let tokens = [
            token(TokenKind::Constant, 0, 3),
            token(TokenKind::Other, 3, 4),
        ];
        assert_eq!(render(sql, &tokens).as_deref(), Some("$1 b"));
        // Punctuation needs no separator.
        let sql = "(1)";
        let tokens = [
            token(TokenKind::Other, 0, 1),
            token(TokenKind::Constant, 1, 2),
            token(TokenKind::Other, 2, 3),
        ];
        assert_eq!(render(sql, &tokens).as_deref(), Some("($1)"));
    }

    #[test]
    fn render_rejects_inconsistent_tokens() {
        let overlapping = [token(TokenKind::Other, 0, 3), token(TokenKind::Other, 2, 4)];
        assert_eq!(render("abcd", &overlapping), None);
        let out_of_bounds = [token(TokenKind::Other, 0, 9)];
        assert_eq!(render("abcd", &out_of_bounds), None);
        // Splits the two-byte "é".
        let mid_char = [token(TokenKind::Other, 0, 1)];
        assert_eq!(render("é", &mid_char), None);
    }

    #[test]
    fn render_of_no_tokens_is_empty() {
        assert_eq!(render("  -- c", &[]).as_deref(), Some(""));
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::ffi::CString;

    use pgrx::prelude::*;

    use super::*;

    fn norm(sql: &str) -> String {
        normalize(sql).unwrap_or_else(|| panic!("normalize returned None for {sql:?}"))
    }

    fn statement(source: &str, location: i32, len: i32) -> Option<String> {
        let source = CString::new(source).expect("no NUL in test input");
        // SAFETY: `source` is a valid NUL-terminated string.
        unsafe { current_statement(source.as_ptr(), location, len) }
    }

    #[pg_test]
    fn masks_string_literals() {
        assert_eq!(
            norm("SELECT * FROM t WHERE name = 'secret' AND x = 'it''s'"),
            "SELECT * FROM t WHERE name = $1 AND x = $2"
        );
    }

    #[pg_test]
    fn masks_escape_strings() {
        let out = norm(r"SELECT E'sec\'ret\n', e'other'");
        assert_eq!(out, "SELECT $1, $2");
        assert!(!out.contains("sec"));
    }

    #[pg_test]
    fn masks_unicode_escape_strings() {
        let out = norm(r"SELECT U&'d\0061ta secret', U&'!0061' UESCAPE '!'");
        assert_eq!(out, "SELECT $1, $2 UESCAPE $3");
    }

    #[pg_test]
    fn masks_dollar_quoted_strings() {
        let out = norm("SELECT $$sec'ret$$, $tag$ in$$ner 'x' $tag$ FROM t");
        assert_eq!(out, "SELECT $1, $2 FROM t");
    }

    #[pg_test]
    fn masks_bit_and_hex_strings() {
        let out = norm("SELECT B'101010', b'1', X'DEADBEEF', x'ff'");
        assert_eq!(out, "SELECT $1, $2, $3, $4");
    }

    #[pg_test]
    fn masks_numbers() {
        let out = norm("SELECT 42, 3.14159, 1e10, .5, 0x1F, 1_000, 99999999999999999999");
        assert_eq!(out, "SELECT $1, $2, $3, $4, $5, $6, $7");
    }

    #[pg_test]
    fn negative_numbers_keep_only_the_operator() {
        let out = norm("SELECT * FROM t WHERE a = -12345 AND b > -0.5");
        assert_eq!(out, "SELECT * FROM t WHERE a = -$1 AND b > -$2");
        assert!(!out.contains("12345"));
    }

    #[pg_test]
    fn typed_literals_are_masked() {
        let out = norm("SELECT DATE '2020-01-02', INTERVAL '1 day', 'x'::text");
        assert_eq!(out, "SELECT DATE $1, INTERVAL $2, $3::text");
    }

    #[pg_test]
    fn drops_nested_block_comments() {
        let out = norm("SELECT /* a /* nested 'secret' */ still comment */ 1 FROM t");
        assert_eq!(out, "SELECT $1 FROM t");
    }

    #[pg_test]
    fn drops_line_comments() {
        let out = norm("SELECT 1 -- trailing 'secret'\nFROM t -- end");
        assert_eq!(out, "SELECT $1 FROM t");
    }

    #[pg_test]
    fn comment_separates_tokens_and_whitespace_collapses() {
        assert_eq!(norm("SELECT/**/a  \n\t FROM    t"), "SELECT a FROM t");
        assert_eq!(norm("  SELECT 1  "), "SELECT $1");
    }

    #[pg_test]
    fn comment_markers_inside_literals_are_masked_not_stripped() {
        let out = norm("SELECT '-- not a comment', '/* nor this */' FROM t");
        assert_eq!(out, "SELECT $1, $2 FROM t");
    }

    #[pg_test]
    fn adjacent_string_continuation_is_one_constant() {
        let out = norm("SELECT 'abc'\n'def' FROM t");
        assert_eq!(out, "SELECT $1 FROM t");
    }

    #[pg_test]
    fn existing_parameters_are_kept_and_placeholders_do_not_collide() {
        let out = norm("SELECT * FROM t WHERE a = $1 AND b = 'x' AND c = $3");
        assert_eq!(out, "SELECT * FROM t WHERE a = $1 AND b = $4 AND c = $3");
    }

    #[pg_test]
    fn operators_and_identifiers_survive() {
        let out = norm(r#"SELECT "Quoted Ident", a->>'k', a::int, b<=c FROM s.t"#);
        assert_eq!(
            out,
            r#"SELECT "Quoted Ident", a->>$1, a::int, b<=c FROM s.t"#
        );
    }

    #[pg_test]
    fn multibyte_text_outside_literals_is_preserved() {
        let out = norm("SELECT héllo FROM tàble WHERE x = 'é'");
        assert_eq!(out, "SELECT héllo FROM tàble WHERE x = $1");
    }

    #[pg_test]
    fn empty_and_comment_only_input_normalize_to_empty() {
        assert_eq!(norm(""), "");
        assert_eq!(norm("-- nothing here"), "");
    }

    #[pg_test]
    fn malformed_input_returns_none_and_does_not_raise() {
        assert_eq!(normalize("SELECT 'unterminated secret"), None);
        assert_eq!(normalize("SELECT $$unterminated secret"), None);
        assert_eq!(normalize("SELECT /* unterminated secret"), None);
        assert_eq!(normalize("SELECT \"unterminated secret"), None);
        assert_eq!(normalize("SELECT E'\\uZZZZ secret'"), None);
        assert_eq!(normalize("SELECT 'has\0nul'"), None);
        // The backend must still be fully usable after caught scanner errors.
        assert_eq!(norm("SELECT 1"), "SELECT $1");
        let one = Spi::get_one::<i32>("SELECT 1").expect("spi works");
        assert_eq!(one, Some(1));
    }

    #[pg_test]
    fn current_statement_extracts_one_statement() {
        let source = "SELECT 'a'; UPDATE t SET x = 'b' ;  DELETE FROM u";
        let second_start = source.find("UPDATE").expect("present") as i32;
        let second_len = source.find(" ;").expect("present") as i32 - second_start;
        assert_eq!(
            statement(source, second_start, second_len).as_deref(),
            Some("UPDATE t SET x = 'b'")
        );
        // Length 0 means "rest of the string".
        let third_start = source.find("DELETE").expect("present") as i32;
        assert_eq!(
            statement(source, third_start, 0).as_deref(),
            Some("DELETE FROM u")
        );
        // Unknown location means the whole (trimmed) string.
        assert_eq!(
            statement("  SELECT 1  ", -1, 0).as_deref(),
            Some("SELECT 1")
        );
    }

    #[pg_test]
    fn current_statement_handles_null_and_bad_ranges() {
        // SAFETY: NULL is explicitly allowed.
        assert_eq!(unsafe { current_statement(std::ptr::null(), 0, 0) }, None);
        assert_eq!(statement("SELECT 1", 100, 0), None);
        assert_eq!(statement("SELECT 1", 0, 100).as_deref(), Some("SELECT 1"));
        assert_eq!(statement("   ", 0, 0), None);
        assert_eq!(statement("", -1, 0), None);
    }

    #[pg_test]
    fn current_statement_is_lossy_for_invalid_utf8() {
        let source = CString::new(vec![b'S', b' ', 0xff, b' ', b'x']).expect("no NUL");
        // SAFETY: valid NUL-terminated string.
        let out = unsafe { current_statement(source.as_ptr(), -1, 0) };
        assert_eq!(out.as_deref(), Some("S \u{fffd} x"));
    }

    #[pg_test]
    fn sanitize_combines_extract_normalize_truncate() {
        let text = "SELECT 1; SELECT 'topsecret', 22 FROM t; SELECT 3";
        let source = CString::new(text).expect("no NUL in test input");
        let start = text.find("SELECT 'topsecret'").expect("present") as i32;
        let len = text.find("; SELECT 3").expect("present") as i32 - start;
        // SAFETY: `source` is a valid NUL-terminated string.
        let (normalized, raw, short) = unsafe {
            (
                sanitize(source.as_ptr(), start, len, true, 1024),
                sanitize(source.as_ptr(), start, len, false, 1024),
                sanitize(source.as_ptr(), start, len, true, 8),
            )
        };
        assert_eq!(normalized.as_deref(), Some("SELECT $1, $2 FROM t"));
        assert_eq!(raw.as_deref(), Some("SELECT 'topsecret', 22 FROM t"));
        assert_eq!(short.as_deref(), Some("SELECT $"));
    }

    #[pg_test]
    fn sanitize_never_leaks_when_normalization_fails() {
        let source = CString::new("SELECT 'topsecret").expect("no NUL in test input");
        // SAFETY: `source` is a valid NUL-terminated string.
        let out = unsafe { sanitize(source.as_ptr(), -1, 0, true, 1024) };
        assert_eq!(out, None);
    }

    #[pg_test]
    fn literal_content_never_appears_in_output() {
        let secrets = [
            "hunter2",
            "4111111111111111",
            "s3cr3t",
            "DEADBEEF",
            "0xCAFE",
        ];
        let sql = "INSERT INTO users(name, pw, card, flags, h) VALUES \
                   ('hunter2', E'hunter2\\'', 4111111111111111, B'1', X'DEADBEEF'); \
                   -- s3cr3t\n\
                   SELECT $$s3cr3t$$, $q$s3cr3t$q$, U&'s3cr3t', 0xCAFE, -4111111111111111";
        let out = norm(sql);
        for secret in secrets {
            assert!(!out.contains(secret), "{secret} leaked in {out:?}");
        }
        assert!(!out.contains("--"));
    }

    #[pg_test]
    fn national_literal_does_not_fuse_with_placeholder() {
        let out = norm("SELECT N'secret', n'secret2' FROM t");
        assert_eq!(out, "SELECT N $1, n $2 FROM t");
    }

    #[pg_test]
    fn do_block_body_is_masked_as_a_whole() {
        let out = norm("DO $$ BEGIN PERFORM 'secret'; END $$ LANGUAGE plpgsql");
        assert_eq!(out, "DO $1 LANGUAGE plpgsql");
    }

    #[pg_test]
    fn long_identifiers_normalize_without_error() {
        let long = "a".repeat(100);
        let out = norm(&format!("SELECT {long} FROM {long}_t WHERE x = 'secret'"));
        assert_eq!(out, format!("SELECT {long} FROM {long}_t WHERE x = $1"));
    }

    #[pg_test]
    fn message_levels_are_restored_after_scanning() {
        let client = unsafe { pg_sys::client_min_messages };
        let log_level = unsafe { MessageSuppression::log_min_messages() };
        let _ = normalize("SELECT 1");
        let _ = normalize("SELECT 'unterminated");
        assert_eq!(unsafe { pg_sys::client_min_messages }, client);
        assert_eq!(unsafe { MessageSuppression::log_min_messages() }, log_level);
        {
            let _quiet = MessageSuppression::new();
            assert!(unsafe { pg_sys::client_min_messages } >= pgrx::PgLogLevel::ERROR as c_int);
        }
        assert_eq!(unsafe { pg_sys::client_min_messages }, client);
    }

    #[pg_test]
    fn oversized_input_fails_closed() {
        let ok = format!("SELECT '{}'", "a".repeat(MAX_NORMALIZE_INPUT_BYTES - 10));
        assert!(ok.len() <= MAX_NORMALIZE_INPUT_BYTES);
        assert_eq!(norm(&ok), "SELECT $1");
        let too_big = format!("SELECT '{}'", "a".repeat(MAX_NORMALIZE_INPUT_BYTES));
        assert_eq!(normalize(&too_big), None);
    }

    #[pg_test]
    fn current_statement_treats_negative_length_as_rest_of_string() {
        let source = "SELECT 1; SELECT 2";
        let start = source.find("SELECT 2").expect("present") as i32;
        assert_eq!(statement(source, start, -5).as_deref(), Some("SELECT 2"));
        // Length equal to the rest of the string is the exact boundary.
        let rest = (source.len() as i32) - start;
        assert_eq!(statement(source, start, rest).as_deref(), Some("SELECT 2"));
        assert_eq!(
            statement(source, start, rest + 1).as_deref(),
            Some("SELECT 2")
        );
    }

    /// Random alphanumeric literal bodies, in every literal form, must never
    /// show up in the normalized text, whether they sit in a literal, a
    /// comment, or after a unary minus.
    #[pg_test]
    fn random_literal_bodies_never_leak() {
        const ALNUM: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        let mut rng = fastrand::Rng::with_seed(0x5eed_cafe);
        let mut body = |alphabet: &[u8], len: usize| -> String {
            (0..len)
                .map(|_| alphabet[rng.usize(..alphabet.len())] as char)
                .collect()
        };

        for _ in 0..200 {
            let word = body(ALNUM, 16);
            let bits = body(b"01", 24);
            let hex = body(b"0123456789abcdef", 20);
            let digits = body(b"123456789", 18);
            let sql = format!(
                "SELECT '{word}', E'{word}', U&'{word}', N'{word}', $${word}$$, \
                 $t${word}$t$, B'{bits}', X'{hex}', {digits}, -{digits}, {digits}.5, \
                 DATE '{word}' /* {word} /* {word} */ */ -- {word}\n\
                 FROM t WHERE c = '{word}' 'more {word}'"
            );
            let out = norm(&sql);
            for secret in [&word, &bits, &hex, &digits] {
                assert!(!out.contains(secret.as_str()), "{secret} leaked in {out:?}");
            }
            assert!(!out.contains("--") && !out.contains("/*"), "{out:?}");
        }
    }
}
