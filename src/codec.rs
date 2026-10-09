//! Compact binary encoding of [`SpanRecord`]s for the shared-memory queue.
//!
//! The encoding is private to this extension: producers (backends) and the
//! consumer (the exporter worker) run the same binary, so there is no
//! compatibility story beyond a one-byte format version that makes a decoder
//! reject records it does not understand (for example after a crash restart
//! mixed old and new data, which cannot happen because shared memory is reset).
//!
//! All integers are little-endian; strings are a `u32` byte length followed by
//! UTF-8 bytes; optional values are a `0`/`1` byte followed by the value.
//! Decoding validates every length against the bytes that are really present,
//! so corrupt input yields a [`DecodeError`], never a panic or an over-read.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::{SpanId, TraceId};

use crate::{
    queue::{Batch, records},
    span::{PlanNodeAttributes, QueryAttributes, SpanAttributes, SpanRecord},
};

const FORMAT_VERSION: u8 = 2;
const KIND_QUERY: u8 = 1;
const KIND_PLAN_NODE: u8 = 2;

#[derive(Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// The input ended before the value was complete.
    Truncated,
    /// A string was not valid UTF-8.
    InvalidUtf8,
    /// An unknown format version, span kind or boolean/option tag.
    UnknownTag(u8),
    /// Bytes remained after the record was complete.
    TrailingBytes,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => f.write_str("record is truncated"),
            Self::InvalidUtf8 => f.write_str("string is not valid UTF-8"),
            Self::UnknownTag(tag) => write!(f, "unknown tag {tag}"),
            Self::TrailingBytes => f.write_str("unexpected bytes after the record"),
        }
    }
}

/// Appends the encoding of `span` to `out`.
pub fn encode_span(span: &SpanRecord, out: &mut Vec<u8>) {
    out.push(FORMAT_VERSION);
    out.push(match span.attributes {
        SpanAttributes::Query(_) => KIND_QUERY,
        SpanAttributes::PlanNode(_) => KIND_PLAN_NODE,
    });
    out.extend_from_slice(&span.trace_id.to_bytes());
    out.extend_from_slice(&span.span_id.to_bytes());
    out.extend_from_slice(&span.parent_id.to_bytes());
    put_time(out, span.start_time);
    put_time(out, span.end_time);
    put_str(out, &span.name);
    match &span.attributes {
        SpanAttributes::Query(query) => encode_query(query, out),
        SpanAttributes::PlanNode(node) => encode_plan_node(node, out),
    }
}

fn encode_query(query: &QueryAttributes, out: &mut Vec<u8>) {
    put_bool(out, query.parent_is_remote);
    put_str(out, &query.operation);
    put_opt_str(out, query.query_text.as_deref());
    put_i64(out, query.query_id);
    put_i64(out, query.exec_total_time_ns);
    put_u64(out, query.plan_spans_omitted);
}

fn encode_plan_node(node: &PlanNodeAttributes, out: &mut Vec<u8>) {
    put_str(out, &node.node_type);
    put_opt_str(out, node.relation.as_deref());
    put_opt_str(out, node.parent_relationship.as_deref());
    put_opt_str(out, node.subplan_name.as_deref());
    put_u64(out, node.subplans_removed);
    put_opt_i64(out, node.workers_launched);
    put_bool(out, node.never_executed);
    put_bool(out, node.instrumentation_incomplete);
    put_f64(out, node.startup_cost);
    put_f64(out, node.total_cost);
    put_f64(out, node.rows);
    out.extend_from_slice(&node.width_bytes.to_le_bytes());
    put_bool(out, node.parallel_aware);
    put_bool(out, node.parallel_safe);
    put_bool(out, node.async_capable);
    put_i64(out, node.instr_startup_time_ns);
    put_i64(out, node.instr_total_time_ns);
    put_f64(out, node.instr_rows);
    put_f64(out, node.instr_secondary_rows);
    put_f64(out, node.instr_loops);
    put_f64(out, node.instr_rows_removed_by_scan_or_join_filter);
    put_f64(out, node.instr_rows_removed_by_other_filter);
}

/// Decodes one record produced by [`encode_span`].
pub fn decode_span(bytes: &[u8]) -> Result<SpanRecord, DecodeError> {
    let mut reader = Reader { rest: bytes };
    let version = reader.u8()?;
    if version != FORMAT_VERSION {
        return Err(DecodeError::UnknownTag(version));
    }
    let kind = reader.u8()?;
    let trace_id = TraceId::from_bytes(reader.array()?);
    let span_id = SpanId::from_bytes(reader.array()?);
    let parent_id = SpanId::from_bytes(reader.array()?);
    let start_time = reader.time()?;
    let end_time = reader.time()?;
    let name = reader.string()?;
    let attributes = match kind {
        KIND_QUERY => SpanAttributes::Query(decode_query(&mut reader)?),
        KIND_PLAN_NODE => SpanAttributes::PlanNode(decode_plan_node(&mut reader)?),
        other => return Err(DecodeError::UnknownTag(other)),
    };
    if !reader.rest.is_empty() {
        return Err(DecodeError::TrailingBytes);
    }
    Ok(SpanRecord {
        trace_id,
        span_id,
        parent_id,
        name,
        start_time,
        end_time,
        attributes,
    })
}

fn decode_query(reader: &mut Reader) -> Result<QueryAttributes, DecodeError> {
    Ok(QueryAttributes {
        parent_is_remote: reader.bool()?,
        operation: reader.string()?,
        query_text: reader.opt_string()?,
        query_id: reader.i64()?,
        exec_total_time_ns: reader.i64()?,
        plan_spans_omitted: reader.u64()?,
    })
}

fn decode_plan_node(reader: &mut Reader) -> Result<PlanNodeAttributes, DecodeError> {
    Ok(PlanNodeAttributes {
        node_type: reader.string()?,
        relation: reader.opt_string()?,
        parent_relationship: reader.opt_string()?,
        subplan_name: reader.opt_string()?,
        subplans_removed: reader.u64()?,
        workers_launched: reader.opt_i64()?,
        never_executed: reader.bool()?,
        instrumentation_incomplete: reader.bool()?,
        startup_cost: reader.f64()?,
        total_cost: reader.f64()?,
        rows: reader.f64()?,
        width_bytes: i32::from_le_bytes(reader.array()?),
        parallel_aware: reader.bool()?,
        parallel_safe: reader.bool()?,
        async_capable: reader.bool()?,
        instr_startup_time_ns: reader.i64()?,
        instr_total_time_ns: reader.i64()?,
        instr_rows: reader.f64()?,
        instr_secondary_rows: reader.f64()?,
        instr_loops: reader.f64()?,
        instr_rows_removed_by_scan_or_join_filter: reader.f64()?,
        instr_rows_removed_by_other_filter: reader.f64()?,
    })
}

/// Encodes `spans` into one all-or-nothing batch. Spans whose encoding is too
/// large for a queue record are skipped; their number is returned.
pub fn encode_batch(spans: &[SpanRecord]) -> (Batch, usize) {
    let mut batch = Batch::new();
    let mut skipped = 0;
    for span in spans {
        if batch.push_record(|out| encode_span(span, out)).is_err() {
            skipped += 1;
        }
    }
    (batch, skipped)
}

/// Decodes every record of a byte stream drained from the queue. Returns the
/// spans and the number of records that could not be decoded (those are
/// skipped; a corrupt stream ends at the first bad length prefix).
pub fn decode_records(bytes: &[u8]) -> (Vec<SpanRecord>, usize) {
    let mut spans = Vec::new();
    let mut undecodable = 0;
    for payload in records(bytes) {
        match payload
            .map_err(|_| ())
            .and_then(|p| decode_span(p).map_err(|_| ()))
        {
            Ok(span) => spans.push(span),
            Err(()) => undecodable += 1,
        }
    }
    (spans, undecodable)
}

fn put_bool(out: &mut Vec<u8>, value: bool) {
    out.push(u8::from(value));
}

fn put_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_opt_i64(out: &mut Vec<u8>, value: Option<i64>) {
    match value {
        Some(value) => {
            out.push(1);
            put_i64(out, value);
        }
        None => out.push(0),
    }
}

fn put_f64(out: &mut Vec<u8>, value: f64) {
    out.extend_from_slice(&value.to_le_bytes());
}

/// Nanoseconds since the Unix epoch; times before it are clamped to the epoch.
fn put_time(out: &mut Vec<u8>, time: SystemTime) {
    let nanos = time
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since_epoch| since_epoch.as_nanos());
    out.extend_from_slice(&u64::try_from(nanos).unwrap_or(u64::MAX).to_le_bytes());
}

fn put_str(out: &mut Vec<u8>, value: &str) {
    // Strings are bounded far below `u32::MAX` by the callers (and by the
    // record size limit of the queue); saturate rather than wrap if not.
    let len = u32::try_from(value.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&value.as_bytes()[..len as usize]);
}

fn put_opt_str(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            out.push(1);
            put_str(out, value);
        }
        None => out.push(0),
    }
}

struct Reader<'a> {
    rest: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], DecodeError> {
        if len > self.rest.len() {
            return Err(DecodeError::Truncated);
        }
        let (taken, rest) = self.rest.split_at(len);
        self.rest = rest;
        Ok(taken)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let mut array = [0; N];
        array.copy_from_slice(self.take(N)?);
        Ok(array)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.array::<1>()?[0])
    }

    fn bool(&mut self) -> Result<bool, DecodeError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(DecodeError::UnknownTag(other)),
        }
    }

    fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn opt_i64(&mut self) -> Result<Option<i64>, DecodeError> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.i64().map(Some),
            other => Err(DecodeError::UnknownTag(other)),
        }
    }

    fn f64(&mut self) -> Result<f64, DecodeError> {
        Ok(f64::from_le_bytes(self.array()?))
    }

    fn time(&mut self) -> Result<SystemTime, DecodeError> {
        Ok(UNIX_EPOCH + Duration::from_nanos(u64::from_le_bytes(self.array()?)))
    }

    fn string(&mut self) -> Result<String, DecodeError> {
        let len = u32::from_le_bytes(self.array()?) as usize;
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| DecodeError::InvalidUtf8)
    }

    fn opt_string(&mut self) -> Result<Option<String>, DecodeError> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.string().map(Some),
            other => Err(DecodeError::UnknownTag(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query_span() -> SpanRecord {
        SpanRecord {
            trace_id: TraceId::from(0x1122_3344_5566_7788_99aa_bbcc_ddee_ff00_u128),
            span_id: SpanId::from(7),
            parent_id: SpanId::INVALID,
            name: "SELECT public.t".to_owned(),
            start_time: UNIX_EPOCH + Duration::from_nanos(1_700_000_000_123_456_789),
            end_time: UNIX_EPOCH + Duration::from_nanos(1_700_000_000_223_456_789),
            attributes: SpanAttributes::Query(QueryAttributes {
                parent_is_remote: true,
                operation: "SELECT".to_owned(),
                query_text: Some("SELECT * FROM t WHERE x = $1 -- é€😀".to_owned()),
                query_id: -42,
                exec_total_time_ns: 100_000_000,
                plan_spans_omitted: 17,
            }),
        }
    }

    fn plan_span() -> SpanRecord {
        SpanRecord {
            trace_id: TraceId::from(5),
            span_id: SpanId::from(9),
            parent_id: SpanId::from(7),
            name: "postgresql.operation.T_SeqScanState [public.t]".to_owned(),
            start_time: UNIX_EPOCH + Duration::from_secs(5),
            end_time: UNIX_EPOCH + Duration::from_secs(6),
            attributes: SpanAttributes::PlanNode(PlanNodeAttributes {
                node_type: "T_SeqScanState".to_owned(),
                relation: Some("public.t".to_owned()),
                parent_relationship: Some("Member".to_owned()),
                subplan_name: Some("InitPlan 1".to_owned()),
                subplans_removed: 2,
                workers_launched: Some(3),
                never_executed: true,
                instrumentation_incomplete: true,
                startup_cost: 0.0,
                total_cost: 35.5,
                rows: 2550.0,
                width_bytes: -4,
                parallel_aware: true,
                parallel_safe: false,
                async_capable: true,
                instr_startup_time_ns: 11,
                instr_total_time_ns: 22,
                instr_rows: 10.0,
                instr_secondary_rows: 0.5,
                instr_loops: 3.0,
                instr_rows_removed_by_scan_or_join_filter: 1.0,
                instr_rows_removed_by_other_filter: f64::MAX,
            }),
        }
    }

    fn encoded(span: &SpanRecord) -> Vec<u8> {
        let mut out = Vec::new();
        encode_span(span, &mut out);
        out
    }

    #[test]
    fn query_span_round_trips() {
        let span = query_span();
        assert_eq!(decode_span(&encoded(&span)), Ok(span));
    }

    #[test]
    fn plan_span_round_trips() {
        let span = plan_span();
        assert_eq!(decode_span(&encoded(&span)), Ok(span));
    }

    #[test]
    fn optional_fields_round_trip_when_absent() {
        let mut span = query_span();
        if let SpanAttributes::Query(query) = &mut span.attributes {
            query.query_text = None;
        }
        assert_eq!(decode_span(&encoded(&span)), Ok(span));
        let mut span = plan_span();
        if let SpanAttributes::PlanNode(node) = &mut span.attributes {
            node.relation = None;
            node.parent_relationship = None;
            node.subplan_name = None;
            node.workers_launched = None;
            node.subplans_removed = 0;
            node.never_executed = false;
            node.instrumentation_incomplete = false;
        }
        assert_eq!(decode_span(&encoded(&span)), Ok(span));
    }

    #[test]
    fn every_truncation_is_an_error_not_a_panic() {
        for span in [query_span(), plan_span()] {
            let bytes = encoded(&span);
            for len in 0..bytes.len() {
                assert!(decode_span(&bytes[..len]).is_err(), "len {len}");
            }
        }
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut bytes = encoded(&query_span());
        bytes.push(0);
        assert_eq!(decode_span(&bytes), Err(DecodeError::TrailingBytes));
    }

    #[test]
    fn unknown_version_kind_and_tags_are_rejected() {
        let good = encoded(&query_span());
        let mut bad_version = good.clone();
        bad_version[0] = 99;
        assert_eq!(decode_span(&bad_version), Err(DecodeError::UnknownTag(99)));
        let mut bad_kind = good.clone();
        bad_kind[1] = 77;
        assert_eq!(decode_span(&bad_kind), Err(DecodeError::UnknownTag(77)));
    }

    #[test]
    fn oversized_string_length_is_truncation() {
        let mut bytes = encoded(&query_span());
        // The span name length prefix follows version, kind, ids and times.
        let name_len_at = 2 + 16 + 8 + 8 + 8 + 8;
        bytes[name_len_at..name_len_at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(decode_span(&bytes), Err(DecodeError::Truncated));
    }

    #[test]
    fn invalid_utf8_is_rejected() {
        let mut span = query_span();
        span.name = "abc".to_owned();
        let mut bytes = encoded(&span);
        let name_at = 2 + 16 + 8 + 8 + 8 + 8 + 4;
        bytes[name_at] = 0xff;
        assert_eq!(decode_span(&bytes), Err(DecodeError::InvalidUtf8));
    }

    #[test]
    fn random_garbage_never_panics() {
        let mut rng = fastrand::Rng::with_seed(1);
        let mut bytes = encoded(&plan_span());
        for _ in 0..5_000 {
            let mut garbage = bytes.clone();
            for _ in 0..rng.usize(1..6) {
                let at = rng.usize(0..garbage.len());
                garbage[at] = rng.u8(..);
            }
            let _ = decode_span(&garbage);
            bytes.rotate_left(1);
            let _ = decode_span(&bytes);
        }
    }

    #[test]
    fn batch_round_trips_through_the_record_stream() {
        let spans = vec![plan_span(), query_span()];
        let (batch, skipped) = encode_batch(&spans);
        assert_eq!(skipped, 0);
        assert_eq!(batch.records(), 2);
        let decoded: Vec<_> = records(batch.as_bytes())
            .map(|payload| decode_span(payload.unwrap()).unwrap())
            .collect();
        assert_eq!(decoded, spans);
    }

    #[test]
    fn spans_too_large_for_a_record_are_skipped() {
        let mut huge = query_span();
        huge.name = "x".repeat(crate::queue::MAX_RECORD_BYTES + 1);
        let (batch, skipped) = encode_batch(&[huge, plan_span()]);
        assert_eq!(skipped, 1);
        assert_eq!(batch.records(), 1);
    }

    #[test]
    fn decode_records_skips_undecodable_records() {
        let mut batch = Batch::new();
        batch
            .push_record(|out| encode_span(&plan_span(), out))
            .unwrap();
        batch
            .push_record(|out| out.extend_from_slice(b"not a span"))
            .unwrap();
        batch
            .push_record(|out| encode_span(&query_span(), out))
            .unwrap();
        let (spans, undecodable) = decode_records(batch.as_bytes());
        assert_eq!(spans, vec![plan_span(), query_span()]);
        assert_eq!(undecodable, 1);
    }

    #[test]
    fn decode_records_stops_at_a_corrupt_stream() {
        let mut bytes = encode_batch(&[plan_span()]).0.as_bytes().to_vec();
        bytes.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 1]);
        let (spans, undecodable) = decode_records(&bytes);
        assert_eq!(spans.len(), 1);
        assert_eq!(undecodable, 1);
    }

    #[test]
    fn times_before_the_epoch_clamp_to_the_epoch() {
        let mut span = plan_span();
        span.start_time = UNIX_EPOCH - Duration::from_secs(1);
        let decoded = decode_span(&encoded(&span)).unwrap();
        assert_eq!(decoded.start_time, UNIX_EPOCH);
    }
}
