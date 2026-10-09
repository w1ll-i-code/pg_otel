-- Update script for pg_otel 0.1.0 -> 0.2.0: adds the dropped span counter.
-- The definition matches what `cargo pgrx schema` generates for 0.2.0.
CREATE FUNCTION "pg_otel_dropped_spans"() RETURNS bigint /* i64 */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pg_otel_dropped_spans_wrapper';
