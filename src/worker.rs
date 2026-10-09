use std::{
    collections::HashMap,
    ffi::CStr,
    fs,
    time::{Duration, Instant},
};

use opentelemetry::KeyValue;
use opentelemetry_otlp::{SpanExporter, WithExportConfig, WithHttpConfig, WithTonicConfig};
use opentelemetry_sdk::{
    Resource,
    trace::{SpanData, SpanExporter as _},
};
use pgrx::{bgworkers::BackgroundWorker, debug1, log, pg_sys};
use tokio::runtime::Runtime;
use tonic::{
    metadata::MetadataMap,
    transport::{Certificate, ClientTlsConfig},
};

use crate::{codec::decode_records, config::ExporterConfig, shared};

/// How long the worker sleeps when nobody wakes it. Backends set its latch when
/// the queue was empty or is getting full, so this only bounds the latency of
/// spans queued without a wake-up.
const IDLE_TIMEOUT: Duration = Duration::from_secs(1);

/// Upper bound of queue bytes copied out (and exported) per round trip. The
/// queue lock is only held while copying this out, never while exporting.
const CHUNK_BYTES: usize = 256 * 1024;

/// On shutdown the remaining spans are exported for at most this long.
const SHUTDOWN_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

/// The number of dropped spans is logged at most this often.
const DROPPED_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Signals that were received but not yet acted upon.
///
/// `BackgroundWorker::sigterm_received` and `sighup_received` *consume* the
/// flag, so whoever reads one must remember the answer; otherwise a shutdown
/// request noticed in the middle of an export would be lost.
#[derive(Debug, Default)]
struct PendingSignals {
    sighup: bool,
    sigterm: bool,
}

impl PendingSignals {
    /// Folds newly received signals into the pending set.
    fn poll(&mut self) {
        self.sighup |= BackgroundWorker::sighup_received();
        self.sigterm |= BackgroundWorker::sigterm_received();
    }

    fn any(&self) -> bool {
        self.sighup || self.sigterm
    }
}

/// Runs until SIGTERM (or postmaster death), then flushes the queue.
///
/// Each wake-up drains the queue completely (producers only wake the worker
/// when the queue was empty or is getting full, so leaving data behind would
/// strand it), but stops between chunks when a signal arrives so a slow
/// collector cannot delay a reload or shutdown by more than one export.
pub fn background_worker_run() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread tokio runtime can always be built");

    let mut exporter = runtime.block_on(build_exporter());
    let mut dropped_report = DroppedSpansReport::new(Instant::now());
    let mut signals = PendingSignals::default();

    loop {
        // Only sleep when there is nothing to react to. `wait_latch` returns
        // false after consuming a SIGTERM, or when the postmaster died.
        if !signals.any() && !BackgroundWorker::wait_latch(Some(IDLE_TIMEOUT)) {
            signals.sigterm = true;
        }
        signals.poll();

        if signals.sigterm {
            break;
        }
        if std::mem::take(&mut signals.sighup) {
            reload_exporter(&runtime, &mut exporter);
        }

        runtime.block_on(export_pending(exporter.as_mut(), &mut signals));
        dropped_report.log_if_due(Instant::now(), shared::dropped_spans());
    }

    // Hand over what is still queued.
    runtime.block_on(flush_and_shutdown(exporter));
}

/// Rebuilds the exporter from the current GUC values. The old exporter is only
/// shut down once its replacement exists, so a bad configuration keeps
/// exporting with the previous one.
fn reload_exporter(runtime: &Runtime, exporter: &mut Option<SpanExporter>) {
    // SIGHUP only sets the worker's flag. Reload this process's local GUC
    // values before taking the configuration snapshot.
    unsafe { pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP) };

    let Some(new_exporter) = runtime.block_on(build_exporter()) else {
        log!("Keeping the existing OTLP exporter after configuration reload failure");
        return;
    };
    if let Some(old_exporter) = exporter.replace(new_exporter) {
        runtime.block_on(shutdown_exporter(&old_exporter));
    }
    log!("Reloaded OTLP exporter configuration");
}

async fn shutdown_exporter(exporter: &SpanExporter) {
    if let Err(error) = exporter.shutdown() {
        debug1!("Could not shut down the OTLP exporter cleanly: {error}");
    }
}

/// Exports everything still queued, but for no longer than
/// [`SHUTDOWN_FLUSH_TIMEOUT`], then shuts the exporter down.
///
/// The time limit is enforced twice: by checking the clock between chunks (an
/// exporter that never awaits, or no exporter at all, would otherwise never
/// give the timeout a chance to fire) and by `tokio::time::timeout` around a
/// single export that hangs.
async fn flush_and_shutdown(mut exporter: Option<SpanExporter>) {
    let deadline = Instant::now() + SHUTDOWN_FLUSH_TIMEOUT;
    let mut buffer = Vec::new();
    let flush = drain_queue(
        || take_chunk(&mut buffer),
        async |spans| export_chunk(exporter.as_mut(), spans).await,
        || Instant::now() >= deadline,
    );
    match tokio::time::timeout(SHUTDOWN_FLUSH_TIMEOUT, flush).await {
        Ok(Drained::Empty) => {}
        Ok(Drained::Interrupted) | Err(_) => {
            log!("Gave up exporting queued spans on shutdown after {SHUTDOWN_FLUSH_TIMEOUT:?}");
        }
    }
    if let Some(exporter) = &exporter {
        shutdown_exporter(exporter).await;
    }
}

/// Remembers how many spans were dropped when it last reported, so the log
/// shows the change instead of a running total only.
struct DroppedSpansReport {
    last_logged_at: Instant,
    last_total: u64,
}

impl DroppedSpansReport {
    fn new(now: Instant) -> Self {
        Self {
            last_logged_at: now,
            last_total: 0,
        }
    }

    /// Logs `total - last_total` if there is news and the last report is at
    /// least [`DROPPED_LOG_INTERVAL`] old.
    fn log_if_due(&mut self, now: Instant, total: u64) {
        if let Some(message) = self.report(now, total) {
            log!("{message}");
        }
    }

    fn report(&mut self, now: Instant, total: u64) -> Option<String> {
        let newly_dropped = total.saturating_sub(self.last_total);
        if newly_dropped == 0 || now.duration_since(self.last_logged_at) < DROPPED_LOG_INTERVAL {
            return None;
        }
        self.last_logged_at = now;
        self.last_total = total;
        Some(format!(
            "Dropped {newly_dropped} spans because the span queue was full ({total} since server start); consider raising pg_otel.queue_size_kb"
        ))
    }
}

async fn build_exporter() -> Option<SpanExporter> {
    let config = ExporterConfig::load()?;
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
                    KeyValue::new("service.framework.name", "postgresql"),
                    KeyValue::new("service.framework.version", server_version()),
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

/// The running server's `server_version` (for example `19beta1`).
fn server_version() -> String {
    // SAFETY: `GetConfigOption` returns null or a NUL-terminated string owned by
    // the GUC machinery; it is copied immediately. With `missing_ok` it never
    // raises an error.
    let version = unsafe {
        let value = pg_sys::GetConfigOption(c"server_version".as_ptr(), true, false);
        (!value.is_null()).then(|| CStr::from_ptr(value).to_string_lossy().into_owned())
    };
    version.unwrap_or_else(|| pg_sys::PG_VERSION.to_string_lossy().into_owned())
}

/// Why [`drain_queue`] returned.
#[derive(Debug, PartialEq, Eq)]
enum Drained {
    /// The queue is empty.
    Empty,
    /// The caller asked to stop; data may remain queued.
    Interrupted,
}

/// Takes chunks from the queue and hands each to `handle` until the queue is
/// empty or `interrupted` says to stop. `interrupted` is checked before every
/// chunk, so a stop request never waits for more than the chunk in flight.
async fn drain_queue<T>(
    mut next_chunk: impl FnMut() -> Vec<T>,
    mut handle: impl AsyncFnMut(Vec<T>),
    mut interrupted: impl FnMut() -> bool,
) -> Drained {
    loop {
        if interrupted() {
            return Drained::Interrupted;
        }
        let chunk = next_chunk();
        if chunk.is_empty() {
            return Drained::Empty;
        }
        handle(chunk).await;
    }
}

/// Exports the queued spans chunk by chunk until the queue is empty or a signal
/// arrives (which is then left in `signals`).
///
/// Each chunk is copied out of the queue under the lock and decoded and
/// exported after the lock is released. Without an exporter (misconfiguration)
/// the queue is still drained so it cannot fill up and block newer spans.
async fn export_pending(mut exporter: Option<&mut SpanExporter>, signals: &mut PendingSignals) {
    let mut buffer = Vec::new();
    drain_queue(
        || take_chunk(&mut buffer),
        async |spans| export_chunk(exporter.as_deref_mut(), spans).await,
        || {
            signals.poll();
            signals.any()
        },
    )
    .await;
}

async fn export_chunk(exporter: Option<&mut SpanExporter>, spans: Vec<SpanData>) {
    match exporter {
        Some(exporter) => export(exporter, spans).await,
        None => debug1!(
            "Discarding {} spans: no OTLP exporter configured",
            spans.len()
        ),
    }
}

async fn export(exporter: &mut SpanExporter, spans: Vec<SpanData>) {
    debug1!("Exporting {} spans", spans.len());
    let count = spans.len();
    if let Err(error) = exporter.export(spans).await {
        log!("Could not export {count} spans: {error}");
    }
}

/// Copies the next chunk out of the queue and decodes it.
fn take_chunk(buffer: &mut Vec<u8>) -> Vec<SpanData> {
    buffer.clear();
    match shared::drain_into(buffer, CHUNK_BYTES) {
        Ok(0) => Vec::new(),
        Ok(_) => {
            let (spans, undecodable) = decode_records(buffer);
            if undecodable > 0 {
                log!("Discarded {undecodable} corrupt span records from the queue");
            }
            spans.into_iter().map(SpanData::from).collect()
        }
        Err(corrupted) => {
            log!("Discarded the span queue contents: {}", corrupted.reason);
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    fn block_on<F: Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future)
    }

    #[test]
    fn drain_takes_every_chunk_without_a_cap() {
        let mut queue: VecDeque<Vec<u8>> = (0..100).map(|i| vec![i as u8; 3]).collect();
        let mut handled = 0;
        let outcome = block_on(drain_queue(
            || queue.pop_front().unwrap_or_default(),
            async |chunk| handled += chunk.len(),
            || false,
        ));
        assert_eq!(outcome, Drained::Empty);
        assert_eq!(handled, 300);
        assert!(queue.is_empty());
    }

    #[test]
    fn drain_stops_between_chunks_when_interrupted() {
        let mut queue: VecDeque<Vec<u8>> = (0..10).map(|_| vec![0; 1]).collect();
        let handled = std::cell::Cell::new(0);
        let mut checks = 0;
        let outcome = block_on(drain_queue(
            || queue.pop_front().unwrap_or_default(),
            async |_chunk| handled.set(handled.get() + 1),
            || {
                checks += 1;
                handled.get() == 3
            },
        ));
        assert_eq!(outcome, Drained::Interrupted);
        // The chunk in flight finished, no further chunk was taken.
        assert_eq!(handled.get(), 3);
        assert_eq!(queue.len(), 7);
        assert_eq!(checks, 4);
    }

    #[test]
    fn drain_does_not_touch_the_queue_when_already_interrupted() {
        let mut taken = 0;
        let outcome = block_on(drain_queue(
            || {
                taken += 1;
                vec![1_u8]
            },
            async |_chunk| {},
            || true,
        ));
        assert_eq!(outcome, Drained::Interrupted);
        assert_eq!(taken, 0);
    }

    #[test]
    fn drain_of_an_empty_queue_returns_empty() {
        let outcome = block_on(drain_queue(
            Vec::<u8>::new,
            async |_chunk| panic!("nothing to handle"),
            || false,
        ));
        assert_eq!(outcome, Drained::Empty);
    }

    #[test]
    fn pending_signals_start_clear() {
        let signals = PendingSignals::default();
        assert!(!signals.any());
        assert!(
            PendingSignals {
                sighup: true,
                sigterm: false
            }
            .any()
        );
        assert!(
            PendingSignals {
                sighup: false,
                sigterm: true
            }
            .any()
        );
    }

    #[test]
    fn dropped_report_is_rate_limited_and_reports_the_delta() {
        let start = Instant::now();
        let mut report = DroppedSpansReport::new(start);
        // Nothing dropped: nothing to say.
        assert_eq!(report.report(start + DROPPED_LOG_INTERVAL, 0), None);
        // News, but too early.
        assert_eq!(report.report(start + Duration::from_secs(10), 5), None);
        // Due: reports everything since the last report.
        let message = report
            .report(start + DROPPED_LOG_INTERVAL, 5)
            .expect("a report is due");
        assert!(message.contains("Dropped 5 spans"), "{message}");
        assert!(message.contains("(5 since server start)"), "{message}");
        // No change since: silent even much later.
        assert_eq!(report.report(start + DROPPED_LOG_INTERVAL * 3, 5), None);
        // New drops need a fresh interval.
        assert_eq!(
            report.report(start + DROPPED_LOG_INTERVAL + Duration::from_secs(1), 9),
            None
        );
        let message = report
            .report(start + DROPPED_LOG_INTERVAL * 2, 9)
            .expect("a second report is due");
        assert!(message.contains("Dropped 4 spans"), "{message}");
    }
}
