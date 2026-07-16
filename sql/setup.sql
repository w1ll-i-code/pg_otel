ALTER SYSTEM SET pg_otel.otlp_endpoint = 'https://<COLLECTOR_HOST>:8200';
ALTER SYSTEM SET pg_otel.otlp_protocol = 'grpc';
ALTER SYSTEM SET pg_otel.otlp_timeout_ms = '5000';
ALTER SYSTEM SET pg_otel.otlp_authorization = 'ApiKey <REDACTED_API_KEY>';
ALTER SYSTEM SET pg_otel.otlp_ca_certificate = '/etc/pki/ca-trust/source/anchors/<CA_CERTIFICATE>.pem';
ALTER SYSTEM SET log_min_duration_statement = '0';

SELECT pg_reload_conf();
