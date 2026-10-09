# PostgreSQL OpenTelemetry Plugin

This plugin for PostgreSQL lets you export the query plan and instrumentation
as OpenTelemetry spans.

This works by attaching to the PostgreSQL executor start and end hooks. In the
start hook, it requests the instrumentation for the query plan for each query.
This will cost some CPU and memory, but the overhead is minimal for most
queries. The end hook then collects all the data and sends it to a background
worker to export the data. This allows the plugin to be as lightweight as
possible. 

To reduce the number of spans being generated and sent, it only sends spans for
statements that take at least `pg_otel.min_duration_ms`. Set it to `0` to send
spans for every statement. The default is `-1`, which **disables tracing**
(no instrumentation is requested either), so you need to set it to enable the
plugin. Parallel workers are not traced separately; the leader reports the
statement.

Collecting spans is best-effort and contained: if it raises an error (or
panics), the spans of that statement are dropped and the failure is written to
the server log once per backend (further failures at `DEBUG1`) instead of
failing the query. A query cancel is still honoured. Failures that Postgres
escalates beyond a normal error cannot be contained.

## Configuration

To enable the plugin, you need to add it to your PostgreSQL configuration file.
Add the following line to your `postgresql.conf` file:

```
shared_preload_libraries = 'pg_otel.so'
```

Then you can point it to the OpenTelemetry collector endpoint using the
`pg_otel` GUCs:

```
pg_otel.otlp_endpoint = 'https://localhost:4317'
pg_otel.otlp_protocol = 'grpc' # supports grpc or http/protobuf
pg_otel.otlp_timeout_ms = 5000
pg_otel.otlp_authorization = 'ApiKey ...' # contents of the Authorization header
pg_otel.otlp_ca_certificate = '' # path to a CA certificate file if you have a custom CA
pg_otel.min_duration_ms = 0 # -1 (default) disables tracing, 0 traces every statement
```

### GUC reference

| GUC | Type | Default | Who can change it | Description |
| --- | --- | --- | --- | --- |
| `pg_otel.otlp_endpoint` | string | `http://localhost:4317` | config file / reload | OTLP collector endpoint. |
| `pg_otel.otlp_protocol` | string | `grpc` | config file / reload | `grpc` or `http/protobuf`. |
| `pg_otel.otlp_timeout_ms` | int (ms) | `10000` | config file / reload | Exporter timeout (1 to 86400000). |
| `pg_otel.otlp_authorization` | string | unset | config file / reload | Value of the `Authorization` header. **Superuser-only**: `SHOW` raises a permission denied error for non-privileged roles (superusers and members of `pg_read_all_settings` can read it), it is hidden from `pg_settings` for them, and it is excluded from `SHOW ALL`. |
| `pg_otel.otlp_ca_certificate` | string | unset | config file / reload | Path to a custom CA certificate. |
| `pg_otel.service_name` | string | `postgresql` | config file / reload | `service.name` resource attribute. |
| `pg_otel.traceparent` | string | unset | any user, per session/transaction | W3C `traceparent` of the parent span. See [Usage](#usage). |
| `pg_otel.query_text` | enum | `normalized` | **superuser** (or roles granted `SET` on the parameter) | `off`: no query text. `normalized`: literals are replaced with placeholders (quoted identifiers such as `"My Table"` are kept as is). `raw`: the query text as received (may contain sensitive data). With `normalized`, databases whose encoding is not UTF-8 (for example `SQL_ASCII`) produce no query text, because normalization fails closed. |
| `pg_otel.min_duration_ms` | int (ms) | `-1` | **superuser** (or roles granted `SET` on the parameter) | Only export spans for statements that run at least this long. `-1` disables tracing, `0` traces every statement. Restricted to superusers so that regular users cannot force tracing of everything. |
| `pg_otel.queue_size_kb` | int (kB) | `1024` | server start only | Size of the shared-memory span queue (64 to 1048576). Needs a restart because shared memory is allocated at startup. |

All `pg_otel.*` names are reserved: `SET` and `ALTER SYSTEM` reject unknown
`pg_otel.*` names, and unknown entries in `postgresql.conf` are warned about
and dropped.

**Migration:** the GUCs `otel.service.name` and `otel.traceparent` were renamed
to `pg_otel.service_name` and `pg_otel.traceparent`. Update `postgresql.conf`
and clients; the old names are no longer used by the extension.

`pg_otel.query_text` is applied to the exported statement: only the statement
being executed is exported (not other statements of a multi-statement string),
cut to 512 bytes. In `off` mode no `db.query.text` attribute is set.

> **Note:** `pg_otel.queue_size_kb` is registered but not yet applied; the queue
> length is still fixed at 1024 spans.

## Usage

To link the generated traces to the correct parent spans, you need to pass the
`traceparent` to postgres. This can be done by setting the `pg_otel.traceparent`
GUC. Prefer `SET LOCAL`, which only lasts until the end of the current
transaction:

```sql
BEGIN;
SET LOCAL pg_otel.traceparent = '<traceparent>';
-- queries here belong to the trace
COMMIT;
```

A plain session-level `SET pg_otel.traceparent = ...` stays in effect for every
following transaction on the connection. With connection poolers (or any
connection that is reused), later and unrelated requests would then be attached
to a stale trace, so only use it if you reset it yourself.

Alternatively, you can add the `traceparent` to a comment in the query, either
in the [sqlcommenter](https://google.github.io/sqlcommenter/) format or as
`pg_otel.traceparent=<value>`:

```sql
SELECT * FROM my_table WHERE id = 1 /*traceparent='00-<trace-id>-<span-id>-01'*/;
SELECT * /* pg_otel.traceparent=<traceparent> */ FROM my_table WHERE id = 1;
```

Values may be single or double quoted and percent-encoded. The GUC takes
precedence over a comment, and invalid values are ignored. Comments are found
with a plain text scan, so comment markers inside string literals also count.

Without a (valid) `traceparent`, each statement starts a new trace. This also
applies to statements run inside functions (nested SPI statements): each gets
its own independent trace id unless `pg_otel.traceparent` is set, in which case
they share it.

If the `traceparent` has the sampled flag cleared (`...-00`), no spans are
exported for the statement, as W3C trace context specifies. Note that this lets
a user suppress tracing of their own statements by supplying such a value.

## Exported spans

Each traced statement produces one query span with one child span per plan
node.

Query span attributes:

| Attribute | Description |
| --- | --- |
| `db.system` | Always `postgresql`. |
| `db.operation` | `SELECT`, `INSERT`, ... |
| `db.query.text` | The statement, according to `pg_otel.query_text`. Absent in `off` mode (and in `normalized` mode when normalization is not possible). |
| `db.query.id` | Postgres' query id; only present when it is computed (`compute_query_id`, for example enabled by `pg_stat_statements`). |
| `postgresql.execution.total_time_seconds` | Time spent executing the statement, in seconds. |
| `span.duration.us` | The same duration in **microseconds**. |

Plan node spans carry `postgresql.plan.*` (planner estimates) and
`postgresql.instrumentation.*` (actual rows, loops, filtered rows, startup and
total time in seconds) attributes, plus `span.duration.us` in microseconds.

Timing notes: Postgres only records accumulated durations, not wall-clock
timestamps, per plan node. The query span starts at the time the statement began
executing (derived from the end time and the measured duration) and plan node
spans all start at that same instant; each ends after its accumulated run time.
The duration covers executor run and finish, not parsing, planning or executor
startup. Nodes interrupted mid-execution are skipped. `EXPLAIN` without
`ANALYZE` is not traced.

## Building

To build the plugin, you need to have Rust and PostgreSQL headers installed.
Clone the repository and run `cargo build` to build the plugin.


## Limitations

Right now, the queue length is hardcoded to 1024 spans. This means that if
you have a large number of concurrent queries, some may be dropped if the queue
is full. Additionally, table and span names are truncated to 64 characters and
the query text is truncated to 512 characters.


## Acknowledgements

This was developed ontop of the shoulders of [pgrx](https://github.com/pgcentralfoundation/pgrx)
Without it, this plugin would not have been possible at this quality and in
this short time (for me at least).
