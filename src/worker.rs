use std::time::Duration;

use opentelemetry_sdk::trace::SpanData;
use pgrx::{bgworkers::BackgroundWorker, log};

use crate::DEQUE;

pub fn background_worker_run() {
    while BackgroundWorker::wait_latch(Some(Duration::from_millis(100))) {
        if BackgroundWorker::sighup_received() {
            // on SIGHUP, you might want to reload some external configuration or something
        }

        loop {
            if BackgroundWorker::sigterm_received() {
                return;
            }

            match DEQUE.exclusive().dequeue() {
                Some(span) => {
                    log!("Dequeued span: {:?}", SpanData::from(span));
                }
                None => break,
            }
        }
    }
}
