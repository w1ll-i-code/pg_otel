ALTER SYSTEM SET pg_otel.otlp_endpoint = 'https://localhost:4317';
ALTER SYSTEM SET pg_otel.otlp_protocol = 'grpc';
ALTER SYSTEM SET pg_otel.otlp_timeout_ms = '5000';
ALTER SYSTEM SET pg_otel.otlp_authorization = 'ApiKey ...';
ALTER SYSTEM SET pg_otel.otlp_ca_certificate = '/path/to/cert.pem';
ALTER SYSTEM SET log_min_duration_statement = '500';

SELECT pg_reload_conf();
