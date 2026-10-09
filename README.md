# PostgreSQL OpenTelemetry Plugin

This plugin for PostgreSQL lets you export the query plan and instrumentation
as OpenTelemetry spans.

This works by attaching to the PostgreSQL executor start and end hooks. In the
start hook, it requests the instrumentation for the query plan for each query.
This will cost some CPU and memory, but the overhead is minimal for most
queries. The end hook then collects all the data and sends it to a background
worker to export the data. This allows the plugin to be as lightweight as
possible. 

To reduce the number of spans being generated and sent, it will check the slow
query log `log_min_duration_statement` setting and only send spans for queries
that take longer than this value. Like the slow query log, you can set this to
to 0 to send all spans or -1 to disable it.

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
```

### GUC reference

| GUC | Type | Default | Who can change it | Description |
| --- | --- | --- | --- | --- |
| `pg_otel.otlp_endpoint` | string | `http://localhost:4317` | config file / reload | OTLP collector endpoint. |
| `pg_otel.otlp_protocol` | string | `grpc` | config file / reload | `grpc` or `http/protobuf`. |
| `pg_otel.otlp_timeout_ms` | int (ms) | `10000` | config file / reload | Exporter timeout (1 to 86400000). |
| `pg_otel.otlp_authorization` | string | unset | config file / reload | Value of the `Authorization` header. **Superuser-only**: hidden from `SHOW`/`pg_settings` for roles that are not superusers or members of `pg_read_all_settings`, and excluded from `SHOW ALL`. |
| `pg_otel.otlp_ca_certificate` | string | unset | config file / reload | Path to a custom CA certificate. |
| `otel.service.name` | string | `postgresql` | config file / reload | `service.name` resource attribute. |
| `pg_otel.traceparent` | string | unset | any user, per session/transaction | W3C `traceparent` of the parent span. See [Usage](#usage). |
| `pg_otel.query_text` | enum | `normalized` | **superuser only** | `off`: no query text. `normalized`: literals are replaced with placeholders (quoted identifiers such as `"My Table"` are kept as is). `raw`: the query text as received (may contain sensitive data). |
| `pg_otel.min_duration_ms` | int (ms) | `-1` | **superuser only** | Only export spans for statements that run at least this long. `-1` disables tracing, `0` traces every statement. Restricted to superusers so that regular users cannot force tracing of everything. |
| `pg_otel.queue_size_kb` | int (kB) | `1024` | server start only | Size of the shared-memory span queue (64 to 1048576). Needs a restart because shared memory is allocated at startup. |

> **Note:** `pg_otel.query_text`, `pg_otel.min_duration_ms` and
> `pg_otel.queue_size_kb` are registered but not yet applied by the executor
> hooks. Until they are wired in, the threshold still comes from
> `log_min_duration_statement` and the queue length is fixed at 1024 spans.

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

Alternatively, you can add the `traceparent` to a comment in the query like so:

```sql
SELECT * /* pg_otel.traceparent=<traceparent> */
FROM my_table
WHERE id = 1;
```

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
