-- Example configuration. Replace the placeholders before running.
-- Never commit real credentials to this file.
ALTER SYSTEM SET pg_otel.otlp_endpoint = 'https://<COLLECTOR_HOST>:<PORT>';
ALTER SYSTEM SET pg_otel.otlp_protocol = 'grpc';
ALTER SYSTEM SET pg_otel.otlp_timeout_ms = '5000';
ALTER SYSTEM SET pg_otel.otlp_authorization = '<AUTHORIZATION_HEADER_VALUE>';
ALTER SYSTEM SET pg_otel.otlp_ca_certificate = '/path/to/ca-certificate.pem';
ALTER SYSTEM SET log_min_duration_statement = '0';

SELECT pg_reload_conf();
