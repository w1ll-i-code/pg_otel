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
`pg_otel` guc's:

```
pg_otel.otlp_endpoint = 'https://localhost:4317';
pg_otel.otlp_protocol = 'grpc'; # support grpc or http/protobuf
pg_otel.otlp_timeout_ms = '5000';
pg_otel.otlp_authorization = 'ApiKey ...'; # Set the contents of the Authorization header
pg_otel.otlp_ca_certificate = ''; # Set the path to the CA certificate file if you have a custom CA
```

## Usage

To link the generated traces to the correct parent spans, you need to pass the
`traceparent` to postgres. This can be done by setting the `pg_otel.traceparent`
GUC like so: 

```sql
SET pg_otel.traceparent = '<traceparent>';
```

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
is full.


## Acknowledgements

This was developed ontop of the shoulders of [pgrx](https://github.com/pgcentralfoundation/pgrx)
Without it, this plugin would not have been possible at this quality and in
this short time (for me at least).
