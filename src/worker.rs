use std::time::Duration;

use opentelemetry_sdk::trace::SpanData;
use pgrx::{bgworkers::BackgroundWorker, log};

use crate::{span::HeaplessSpan, DEQUE};

pub fn background_worker_run() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    while BackgroundWorker::wait_latch(Some(Duration::from_millis(5000))) {
        if BackgroundWorker::sighup_received() {
            // on SIGHUP, you might want to reload some external configuration or something
        }

        runtime.block_on(export());
    }
}

pub async fn export() {
    let spans = drain_queue();

    let mut span_data = vec![];
    for span in spans {
        let span = SpanData::from(span);
        log!("Exporting span: {:?}", span);
        span_data.push(span);
    }

    if !span_data.is_empty() {
        log!("Exporting {} spans", span_data.len());
    }
}

// Just copy the queue contents as quickly as possible to avoid blocking
// queries trying to insert new spans while we're exporting
fn drain_queue() -> Vec<HeaplessSpan> {
    let mut queue = DEQUE.exclusive();
    let mut spans = Vec::new();
    while let Some(span) = queue.dequeue() {
        spans.push(span);
    }
    spans
}
