ALTER SYSTEM SET pg_otel.otlp_endpoint = 'https://rdneteye.si.wp.lan:8200';
ALTER SYSTEM SET pg_otel.otlp_protocol = 'grpc';
ALTER SYSTEM SET pg_otel.otlp_timeout_ms = '5000';
ALTER SYSTEM SET pg_otel.otlp_authorization = 'ApiKey MTlMNmk1TUJlVTBWOFlqVEs1RnQ6emJEUzE5ejlUV3V4aFZ3bms3SzE2UQ==';
ALTER SYSTEM SET pg_otel.otlp_ca_certificate = '/etc/pki/ca-trust/source/anchors/WuerthPhoenix-CA-Chain-Base64-2038-2028.cer';
ALTER SYSTEM SET log_min_duration_statement = '0';

SELECT pg_reload_conf();
