use std::{collections::HashMap, fs, time::Duration};

use opentelemetry_otlp::{SpanExporter, WithExportConfig, WithHttpConfig, WithTonicConfig};
use opentelemetry_sdk::{
    trace::{SpanData, SpanExporter as _},
    Resource,
};
use pgrx::{bgworkers::BackgroundWorker, log};
use tonic::{
    metadata::MetadataMap,
    transport::{Certificate, ClientTlsConfig},
};

use crate::{
    span::HeaplessSpan, DEQUE, OTLP_AUTHORIZATION, OTLP_CA_CERTIFICATE, OTLP_ENDPOINT,
    OTLP_PROTOCOL, OTLP_SERVICE_NAME, OTLP_TIMEOUT_MS,
};

#[derive(Clone, Debug)]
pub struct ExporterConfig {
    pub endpoint: String,
    pub protocol: String,
    pub timeout_ms: u32,
    pub authorization: Option<String>,
    pub ca_certificate: Option<String>,
    pub service_name: String,
}

impl ExporterConfig {
    fn load() -> Result<Self, String> {
        let endpoint = OTLP_ENDPOINT
            .get()
            .ok_or_else(|| "OTLP endpoint is not configured".to_owned())?
            .into_string()
            .map_err(|_| "OTLP endpoint is not valid UTF-8".to_owned())?;
        let protocol = OTLP_PROTOCOL
            .get()
            .ok_or_else(|| "OTLP protocol is not configured".to_owned())?
            .into_string()
            .map_err(|_| "OTLP protocol is not valid UTF-8".to_owned())?;
        let authorization = OTLP_AUTHORIZATION
            .get()
            .map(|value| {
                value
                    .into_string()
                    .map_err(|_| "OTLP authorization is not valid UTF-8".to_owned())
            })
            .transpose()?;
        let ca_certificate = OTLP_CA_CERTIFICATE
            .get()
            .map(|value| {
                value
                    .into_string()
                    .map_err(|_| "OTLP CA certificate path is not valid UTF-8".to_owned())
            })
            .transpose()?;
        let service_name = OTLP_SERVICE_NAME
            .get()
            .and_then(|value| value.into_string().ok())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "postgres".to_owned());
        let config = Self {
            endpoint,
            protocol,
            timeout_ms: OTLP_TIMEOUT_MS.get() as u32,
            authorization,
            ca_certificate,
            service_name,
        };
        log!("OTLP CA certificate path: {:?}", config);

        Ok(config)
    }
}

pub fn background_worker_run() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut exporter = runtime.block_on(build_exporter());

    while BackgroundWorker::wait_latch(Some(Duration::from_millis(5000))) {
        if BackgroundWorker::sighup_received() {
            // SIGHUP only sets the worker's flag. Reload this process's local
            // GUC values before taking the configuration snapshot.
            unsafe { pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP) };

            if let Some(new_exporter) = runtime.block_on(build_exporter()) {
                exporter = Some(new_exporter);
                log!("Reloaded OTLP exporter configuration");
            } else {
                log!("Keeping the existing OTLP exporter after configuration reload failure");
            }
        }

        if let Some(exporter) = exporter.as_mut() {
            runtime.block_on(export(exporter));
        } else {
            let _ = drain_queue();
        }
    }
}

async fn build_exporter() -> Option<SpanExporter> {
    let config = match ExporterConfig::load() {
        Ok(config) => config,
        Err(error) => {
            log!("Could not load OTLP exporter configuration: {}", error);
            return None;
        }
    };

    let endpoint = config.endpoint.clone();
    let timeout = Duration::from_millis(config.timeout_ms as u64);
    let protocol = config.protocol.to_ascii_lowercase();

    let result = match protocol.as_str() {
        "grpc" | "grpc-tonic" => {
            let mut builder = opentelemetry_otlp::SpanExporterBuilder::new()
                .with_tonic()
                .with_endpoint(endpoint)
                .with_timeout(timeout);
            if let Some(path) = config.ca_certificate {
                let pem = match fs::read(&path) {
                    Ok(pem) => pem,
                    Err(error) => {
                        log!("Could not read OTLP CA certificate {:?}: {}", path, error);
                        return None;
                    }
                };
                let tls = ClientTlsConfig::new().ca_certificate(Certificate::from_pem(pem));
                builder = builder.with_tls_config(tls);
            }
            if let Some(authorization) = config.authorization {
                let mut metadata = MetadataMap::new();
                match authorization.parse() {
                    Ok(value) => {
                        metadata.insert("authorization", value);
                        builder = builder.with_metadata(metadata);
                    }
                    Err(error) => {
                        log!("Invalid OTLP authorization header: {}", error);
                        return None;
                    }
                }
            }
            builder.build()
        }
        "http" | "http/protobuf" | "http-binary" => {
            let mut builder = opentelemetry_otlp::SpanExporterBuilder::new()
                .with_http()
                .with_endpoint(endpoint)
                .with_timeout(timeout);
            if let Some(authorization) = config.authorization {
                let mut headers = HashMap::new();
                headers.insert("Authorization".to_owned(), authorization);
                builder = builder.with_headers(headers);
            }
            builder.build()
        }
        _ => {
            log!(
                "Unsupported OTLP protocol {:?}; use grpc or http/protobuf",
                config.protocol
            );
            return None;
        }
    };

    match result {
        Ok(mut exporter) => {
            let resource = Resource::builder()
                .with_service_name(config.service_name)
                .with_attributes([
                    opentelemetry::KeyValue::new("telemetry.sdk.name", "opentelemetry"),
                    opentelemetry::KeyValue::new("telemetry.sdk.language", "rust"),
                    opentelemetry::KeyValue::new(
                        "telemetry.sdk.version",
                        env!("CARGO_PKG_VERSION"),
                    ),
                ])
                .build();
            exporter.set_resource(&resource);
            Some(exporter)
        }
        Err(error) => {
            log!("Could not build OTLP exporter: {}", error);
            None
        }
    }
}

pub async fn export(span_exporter: &mut SpanExporter) {
    let spans = drain_queue();
    let span_data: Vec<_> = spans.into_iter().map(SpanData::from).collect();

    if !span_data.is_empty() {
        log!("Exporting {} spans", span_data.len());
        if let Err(error) = span_exporter.export(span_data).await {
            log!("Could not export spans: {}", error);
        }
    }
}

fn drain_queue() -> Vec<HeaplessSpan> {
    let mut queue = DEQUE.exclusive();
    let mut spans = Vec::new();
    while let Some(span) = queue.dequeue() {
        spans.push(span);
    }
    spans
}
