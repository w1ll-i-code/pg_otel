# PostgreSQL OpenTelemetry Plugin

This plugin for PostgreSQL lets you export the query plan and instrumentation
as OpenTelemetry spans.

This works by attaching to the PostgreSQL executor start and end hooks. In the
start hook, it requests the instrumentation for the query plan for each query.
This will cost some CPU and memory, but the overhead is minimal for most
queries. The end hook then collects all the data and sends it to a background
worker to export the data. This allows the plugin to be as lightweight as
possible.

Requesting instrumentation turns on per-plan-node timers (`INSTRUMENT_TIMER`),
which call the system clock around every row a node produces. That is cheap for
most queries but measurable for queries that process very many rows, which is
why tracing is off unless `pg_otel.min_duration_ms` is set.

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
| `pg_otel.max_plan_spans` | int | `1000` | **superuser** (or roles granted `SET` on the parameter) | Most plan node spans exported per statement (0 to 100000; 0 exports only the query span). Plans with more nodes (for example thousands of partitions) are cut off, see [Exported spans](#exported-spans). |
| `pg_otel.queue_size_kb` | int (kB) | `1024` | server start only | Size of the shared-memory span queue in kilobytes (64 to 1048576). Needs a restart because shared memory is allocated at startup. See [How spans are exported](#how-spans-are-exported). |

All `pg_otel.*` names are reserved: `SET` and `ALTER SYSTEM` reject unknown
`pg_otel.*` names, and unknown entries in `postgresql.conf` are warned about
and dropped.

**Migration:** the GUCs `otel.service.name` and `otel.traceparent` were renamed
to `pg_otel.service_name` and `pg_otel.traceparent`. Update `postgresql.conf`
and clients; the old names are no longer used by the extension.

`pg_otel.query_text` is applied to the exported statement: only the statement
being executed is exported (not other statements of a multi-statement string),
cut to 4096 bytes. In `off` mode no `db.query.text` attribute is set.

## How spans are exported

Backends build all spans of a statement in local memory and then put them in a
byte ring buffer in shared memory (`pg_otel.queue_size_kb`) in one step, taking
a lock only for the copy. A statement's spans are queued all together or not at
all: if they do not fit, they are dropped and counted, never cut in half.

A background worker (`pg_otel exporter`) takes the records out in chunks and
exports them over OTLP. Backends wake it through its latch when the queue was
empty or is more than half full; otherwise it checks once a second. On shutdown
it exports what is still queued for at most five seconds. Postgres restarts the
worker 10 seconds after it exits unexpectedly, and the worker deliberately
exits with a non-zero status when it is terminated (for example with
`pg_terminate_backend()`) so that it comes back instead of exporting silently
stopping. The postmaster does not restart it during a server shutdown, but it
does log `background worker "pg_otel exporter" ... exited with exit code 1` at
`LOG` level on every stop; that message is expected.

The worker empties the queue on every wake-up. A slow or unreachable collector
therefore delays export (each failed export takes up to
`pg_otel.otlp_timeout_ms`) and can make the queue fill up, but never delays a
server shutdown or configuration reload by more than the export in progress.

If the queue overflows (collector too slow or down, or a burst of statements),
spans are dropped. The worker logs the number at most once a minute, and the
total since server start is available in SQL:

```sql
SELECT pg_otel_dropped_spans();
```

`pg_otel_dropped_spans()` was added in version 0.2.0. After installing the new
binaries and restarting the server (the library is preloaded), run
`ALTER EXTENSION pg_otel UPDATE;` in each database that has the extension to
create it.

Raise `pg_otel.queue_size_kb` if this number grows. A single statement whose
spans together exceed the queue size can never be exported.

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
node. The plan is walked the way `EXPLAIN` shows it: outer and inner children,
the live members of `Append`, `MergeAppend`, `BitmapAnd` and `BitmapOr`, the
sub-select of a `SubqueryScan`, the children of a `CustomScan`, and init plans,
CTEs and sub-plans (a sub-plan used by several nodes is reported once). Each
span is a child of the plan node it hangs off in the plan tree, so the trace
has the same shape as the `EXPLAIN` output.

Query span attributes:

| Attribute | Description |
| --- | --- |
| `db.system.name` | Always `postgresql`. |
| `db.operation.name` | `SELECT`, `INSERT`, ... |
| `db.query.text` | The statement, according to `pg_otel.query_text`. Absent in `off` mode (and in `normalized` mode when normalization is not possible). |
| `db.query.id` | Postgres' query id; only present when it is computed (`compute_query_id`, for example enabled by `pg_stat_statements`). |
| `postgresql.execution.total_time_seconds` | Time spent executing the statement, in seconds. |
| `span.duration.us` | The same duration in **microseconds**. |

Plan node spans carry `db.system.name`, `postgresql.plan.*` (planner estimates
and `postgresql.plan.relation`, the `schema.table` this node itself scans, if
any) and `postgresql.instrumentation.*` (actual rows, loops, filtered rows,
startup and total time in seconds) attributes, plus `span.duration.us` in
microseconds. The query span's name lists all relations of the plan nodes that
have a span.

Attributes that describe how a plan node fits into the plan (present when they
apply):

| Attribute | Description |
| --- | --- |
| `postgresql.plan.parent_relationship` | EXPLAIN's "Parent Relationship": `Outer`, `Inner`, `InitPlan`, `SubPlan`, `Member` (of an Append, MergeAppend, BitmapAnd or BitmapOr), `Subquery` (of a SubqueryScan) or `Child` (of a CustomScan). Absent on the top plan node. |
| `postgresql.plan.subplan_name` | `InitPlan 1`, `SubPlan 2` or `CTE name` on the root of a sub-plan. The span name is prefixed with it (`InitPlan 1 → postgresql.operation...`). |
| `postgresql.plan.subplans_removed` | Members of an Append or MergeAppend that run-time partition pruning removed; they have no spans. |
| `postgresql.plan.workers_launched` | Parallel workers launched by a Gather or GatherMerge. The workers' work is already included in the totals of the nodes below it. |
| `postgresql.plan.never_executed` | `true` if the node never ran (for example below `LIMIT 0`, or the inner side of a join with an empty outer side). Such a node still gets a span, as EXPLAIN still lists it, with zero duration and counts. |
| `postgresql.plan.instrumentation_incomplete` | `true` if the node was interrupted while running (for example by a cancel) and its timings and counts could not be read; they are reported as zero. Its children are still reported. |

### Large plans

A statement can have a huge plan, for example a table with thousands of
partitions. At most `pg_otel.max_plan_spans` plan node spans are exported per
statement. The plan is walked depth first, parents before children, so the
nodes that are kept are the first ones in `EXPLAIN` order and form a connected
tree below the query span; the rest are left out. The query span then has
`postgresql.plan.spans_truncated = true` and `postgresql.plan.spans_omitted`
with the number of plan nodes that were left out. Relations that only the
omitted nodes scan do not appear in the query span's name.

Even with the limit, the spans of one statement have to fit into the span queue
together, otherwise all of them are dropped (and counted, see above): size
`pg_otel.queue_size_kb` for at least a few hundred kilobytes if you raise
`pg_otel.max_plan_spans`.

### Breaking changes in the exported data

If you have dashboards or alerts on earlier versions of this extension:

| Before | Now |
| --- | --- |
| `db.statement` | `db.query.text` (sanitized according to `pg_otel.query_text`) |
| `db.system` | `db.system.name` (current OpenTelemetry semantic conventions) |
| `db.operation` | `db.operation.name` |
| `postgresql.plan.tables` (list of all relations below the node) | `postgresql.plan.relation` (only the node's own relation) |
| `postgresql.execution.startup_time_seconds` | removed (it was always 0) |
| `span.duration.us` | now really microseconds (it used to be off by a factor of 1000 or 1e6) |
| span timestamps and durations | converted from Postgres' clock ticks to nanoseconds (they were wrong on machines where the ticks are not nanoseconds) |
| span names | no longer cut to 64 characters (now 256 bytes) |
| plans | nodes below `Append`, `MergeAppend`, `BitmapAnd`/`BitmapOr`, `SubqueryScan`, `CustomScan`, and init plans / sub-plans now have spans (they were missing); the number of spans per statement grows accordingly |

Timing notes: Postgres only records accumulated durations, not wall-clock
timestamps, per plan node. The query span starts at the time the statement began
executing (derived from the end time and the measured duration) and plan node
spans all start at that same instant; each ends after its accumulated run time.
The duration covers executor run and finish, not parsing, planning or executor
startup. `EXPLAIN` without `ANALYZE` is not traced.

## Supported PostgreSQL versions

PostgreSQL **18** and **19**. Exactly one is selected at build time with a cargo
feature; the default is `pg19`. Older versions are not supported (the
instrumentation interfaces this extension relies on differ too much).

| | PostgreSQL 18 | PostgreSQL 19 |
| --- | --- | --- |
| cargo feature | `pg18` | `pg19` (default) |
| `cargo pgrx test` | `cargo pgrx test pg18` | `cargo pgrx test pg19` |

## Building

You need Rust, [`cargo-pgrx`](https://github.com/pgcentralfoundation/pgrx)
(version 0.19.1, matching the `pgrx` dependency, initialised with
`cargo pgrx init` for the Postgres versions you build for) and the PostgreSQL
headers.

For PostgreSQL 19 (the default):

```sh
cargo pgrx install --pg-config /path/to/pg19/bin/pg_config
cargo pgrx package --pg-config /path/to/pg19/bin/pg_config
```

For PostgreSQL 18, turn the default feature off and select `pg18`:

```sh
cargo pgrx install --no-default-features --features pg18 --pg-config /path/to/pg18/bin/pg_config
cargo pgrx package --no-default-features --features pg18 --pg-config /path/to/pg18/bin/pg_config
```

A plain `cargo build` / `cargo clippy` builds for PostgreSQL 19; add
`--no-default-features --features pg18` for 18. Enabling both features, or none,
is an error. The extension must be built separately for each major version.

### Differences between the versions

The exported data and the configuration are the same on both versions. Inside,
the extension adapts to how each version measures time:

* **Timing source.** PostgreSQL 19 counts instrumentation in raw clock ticks
  (TSC ticks on x86-64) which the extension converts to nanoseconds the way
  Postgres does. PostgreSQL 18 already stores nanoseconds (statement timer) and
  seconds as floating point numbers (plan nodes). On PostgreSQL 18 durations of
  plan nodes therefore have the precision of a `double` in seconds, which is
  still far below a nanosecond for realistic durations.
* **Statement timer.** On PostgreSQL 18 the extension adds the statement timer
  (`QueryDesc.totaltime`) right after `ExecutorStart`, like `auto_explain` and
  `pg_stat_statements` do, and uses theirs if they already added one. There is
  only one such timer per statement and whoever adds it first decides what it
  measures, so pg_otel allocates it with all counters (including buffer and WAL
  usage) exactly as those extensions do; `pg_stat_statements` and `auto_explain`
  report the same numbers whatever the order in `shared_preload_libraries`. If
  another extension added a statement timer *without* timing, no spans are
  exported for that statement. PostgreSQL 19 has no such restriction: every
  extension just adds the options it needs to the request and the executor
  allocates the timer.
* **Sub-plan names.** Both versions name sub-plans like EXPLAIN (`InitPlan 1`,
  `SubPlan 2`, `CTE name`).

## Limitations

The span queue has a fixed size (`pg_otel.queue_size_kb`); spans are dropped
when it is full (see [How spans are exported](#how-spans-are-exported)). Span
and relation names are truncated to 256 bytes and the query text to 4096 bytes.
Plans with more than `pg_otel.max_plan_spans` nodes are cut off (see
[Large plans](#large-plans)).


## Acknowledgements

This was developed ontop of the shoulders of [pgrx](https://github.com/pgcentralfoundation/pgrx)
Without it, this plugin would not have been possible at this quality and in
this short time (for me at least).
