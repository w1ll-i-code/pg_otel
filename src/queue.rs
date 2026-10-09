//! Byte ring buffer with length-prefixed records.
//!
//! The ring itself is plain logic over a byte slice and two counters, so it is
//! testable without shared memory; [`crate::shared`] places the slice and the
//! counters in Postgres shared memory and serialises access with an LWLock.
//!
//! # Format
//!
//! Records are stored back to back as `u32` little-endian payload length
//! followed by the payload. They may wrap around the end of the slice. `head`
//! and `tail` are monotonically increasing byte counters (a `u64` does not
//! wrap in practice); the position inside the slice is `counter % capacity`.
//!
//! # Guarantees
//!
//! * Writers push a whole [`Batch`] or nothing ("all-or-nothing"), so the
//!   spans of one statement are never split.
//! * The reader copies out whole records only.
//! * Length prefixes are validated on read. Inconsistent state or a bad length
//!   discards the queued data ([`Corrupted`]) instead of reading out of bounds
//!   or looping.

/// Size of the length prefix of every record.
pub const LEN_PREFIX_BYTES: usize = 4;

/// Largest accepted payload. Doubles as the sanity limit for length prefixes
/// read back from shared memory.
pub const MAX_RECORD_BYTES: usize = 64 * 1024;

/// Read/write positions of a [`Ring`]; lives next to the data in shared memory.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
pub struct RingState {
    head: u64,
    tail: u64,
}

impl RingState {
    pub const fn new() -> Self {
        Self { head: 0, tail: 0 }
    }
}

/// A payload exceeded [`MAX_RECORD_BYTES`].
#[derive(Debug, PartialEq, Eq)]
pub struct RecordTooLarge {
    pub len: usize,
}

/// A batch did not fit into the free space of the ring.
#[derive(Debug, PartialEq, Eq)]
pub struct QueueFull {
    pub needed: usize,
    pub free: usize,
}

/// The ring or a byte stream of records is inconsistent.
#[derive(Debug, PartialEq, Eq)]
pub struct Corrupted {
    pub reason: &'static str,
}

/// Records prepared for a single all-or-nothing push.
///
/// Building a batch needs no lock; only [`Ring::push_batch`] does.
#[derive(Debug, Default)]
pub struct Batch {
    bytes: Vec<u8>,
    records: usize,
}

impl Batch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends one record whose payload is produced by `write` (which must
    /// only append to the vector it is given).
    pub fn push_record(&mut self, write: impl FnOnce(&mut Vec<u8>)) -> Result<(), RecordTooLarge> {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(&[0; LEN_PREFIX_BYTES]);
        write(&mut self.bytes);
        let len = self.bytes.len() - start - LEN_PREFIX_BYTES;
        if len > MAX_RECORD_BYTES {
            self.bytes.truncate(start);
            return Err(RecordTooLarge { len });
        }
        let prefix = (len as u32).to_le_bytes();
        self.bytes[start..start + LEN_PREFIX_BYTES].copy_from_slice(&prefix);
        self.records += 1;
        Ok(())
    }

    /// Number of records in the batch.
    pub fn records(&self) -> usize {
        self.records
    }

    /// Size of the batch including length prefixes.
    pub fn len_bytes(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records == 0
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Single-consumer byte ring over a slice. Callers provide mutual exclusion.
pub struct Ring<'a> {
    state: &'a mut RingState,
    data: &'a mut [u8],
}

impl<'a> Ring<'a> {
    pub fn new(state: &'a mut RingState, data: &'a mut [u8]) -> Self {
        Self { state, data }
    }

    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    /// Bytes currently queued (including length prefixes).
    pub fn used(&self) -> usize {
        if self.is_consistent() {
            (self.state.tail - self.state.head) as usize
        } else {
            0
        }
    }

    pub fn free(&self) -> usize {
        self.capacity() - self.used()
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.used() == 0
    }

    fn is_consistent(&self) -> bool {
        self.state.tail >= self.state.head
            && self.state.tail - self.state.head <= self.data.len() as u64
    }

    fn reset(&mut self) {
        *self.state = RingState::new();
    }

    /// Pushes all records of `batch`, or none if they do not fit.
    ///
    /// Inconsistent state (which only memory corruption could cause) is
    /// repaired by discarding the queue contents first.
    pub fn push_batch(&mut self, batch: &Batch) -> Result<(), QueueFull> {
        if !self.is_consistent() {
            self.reset();
        }
        let needed = batch.len_bytes();
        let free = self.free();
        if needed > free {
            return Err(QueueFull { needed, free });
        }
        self.copy_in(self.state.tail, batch.as_bytes());
        self.state.tail += needed as u64;
        Ok(())
    }

    /// Moves whole records into `out` (with their length prefixes), at most
    /// `max_bytes` in total, but always at least one record if any is queued.
    /// Returns the number of records moved.
    ///
    /// On [`Corrupted`] the queue contents are discarded and `out` is left
    /// untouched.
    pub fn drain_into(&mut self, out: &mut Vec<u8>, max_bytes: usize) -> Result<usize, Corrupted> {
        if !self.is_consistent() {
            self.reset();
            return Err(Corrupted {
                reason: "ring positions are inconsistent",
            });
        }
        match self.measure_drain(max_bytes) {
            Ok((bytes, records)) => {
                self.copy_out(self.state.head, bytes, out);
                self.state.head += bytes as u64;
                Ok(records)
            }
            Err(corrupted) => {
                self.state.head = self.state.tail;
                Err(corrupted)
            }
        }
    }

    /// Walks the length prefixes to find how many bytes/records to take.
    fn measure_drain(&self, max_bytes: usize) -> Result<(usize, usize), Corrupted> {
        let available = self.used();
        let (mut taken, mut records) = (0_usize, 0_usize);
        while taken < available {
            let remaining = available - taken;
            if remaining < LEN_PREFIX_BYTES {
                return Err(Corrupted {
                    reason: "truncated length prefix",
                });
            }
            let len = self.read_len(self.state.head + taken as u64);
            if len > MAX_RECORD_BYTES || LEN_PREFIX_BYTES + len > remaining {
                return Err(Corrupted {
                    reason: "record length out of range",
                });
            }
            let size = LEN_PREFIX_BYTES + len;
            if records > 0 && taken + size > max_bytes {
                break;
            }
            taken += size;
            records += 1;
        }
        Ok((taken, records))
    }

    fn read_len(&self, pos: u64) -> usize {
        let mut prefix = [0_u8; LEN_PREFIX_BYTES];
        self.copy_out_slice(pos, &mut prefix);
        u32::from_le_bytes(prefix) as usize
    }

    fn copy_in(&mut self, pos: u64, src: &[u8]) {
        if src.is_empty() {
            return;
        }
        let capacity = self.data.len();
        let start = (pos % capacity as u64) as usize;
        let first = src.len().min(capacity - start);
        self.data[start..start + first].copy_from_slice(&src[..first]);
        self.data[..src.len() - first].copy_from_slice(&src[first..]);
    }

    fn copy_out(&self, pos: u64, len: usize, out: &mut Vec<u8>) {
        let start = out.len();
        out.resize(start + len, 0);
        self.copy_out_slice(pos, &mut out[start..]);
    }

    fn copy_out_slice(&self, pos: u64, dst: &mut [u8]) {
        if dst.is_empty() {
            return;
        }
        let capacity = self.data.len();
        let start = (pos % capacity as u64) as usize;
        let first = dst.len().min(capacity - start);
        dst[..first].copy_from_slice(&self.data[start..start + first]);
        let rest = dst.len() - first;
        dst[first..].copy_from_slice(&self.data[..rest]);
    }
}

/// Iterates over the payloads of a byte stream produced by
/// [`Ring::drain_into`], validating every length prefix. Iteration ends after
/// the first [`Corrupted`] item.
pub fn records(bytes: &[u8]) -> Records<'_> {
    Records { rest: bytes }
}

pub struct Records<'a> {
    rest: &'a [u8],
}

impl<'a> Iterator for Records<'a> {
    type Item = Result<&'a [u8], Corrupted>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rest.is_empty() {
            return None;
        }
        let item = split_record(self.rest);
        self.rest = match &item {
            Ok((_, rest)) => rest,
            Err(_) => &[],
        };
        Some(item.map(|(payload, _)| payload))
    }
}

fn split_record(bytes: &[u8]) -> Result<(&[u8], &[u8]), Corrupted> {
    let Some((prefix, body)) = bytes.split_first_chunk::<LEN_PREFIX_BYTES>() else {
        return Err(Corrupted {
            reason: "truncated length prefix",
        });
    };
    let len = u32::from_le_bytes(*prefix) as usize;
    if len > MAX_RECORD_BYTES || len > body.len() {
        return Err(Corrupted {
            reason: "record length out of range",
        });
    }
    Ok(body.split_at(len))
}

/// Whether a producer should wake the consumer after a push.
///
/// Wake when the queue was empty (the consumer may be idle) or when the push
/// crossed the 50% high-water mark (the consumer is falling behind). Otherwise
/// the consumer's periodic timeout picks the data up.
pub fn should_wake(used_before: usize, used_after: usize, capacity: usize) -> bool {
    let half = capacity / 2;
    used_before == 0 || (used_before < half && used_after >= half)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    fn batch_of(payloads: &[&[u8]]) -> Batch {
        let mut batch = Batch::new();
        for payload in payloads {
            batch
                .push_record(|buf| buf.extend_from_slice(payload))
                .unwrap();
        }
        batch
    }

    fn drained(ring: &mut Ring, max: usize) -> (usize, Vec<Vec<u8>>) {
        let mut out = Vec::new();
        let count = ring.drain_into(&mut out, max).unwrap();
        let payloads: Vec<_> = records(&out).map(|r| r.unwrap().to_vec()).collect();
        assert_eq!(payloads.len(), count);
        (count, payloads)
    }

    #[test]
    fn empty_ring_drains_nothing() {
        let mut state = RingState::new();
        let mut data = [0_u8; 32];
        let mut ring = Ring::new(&mut state, &mut data);
        assert!(ring.is_empty());
        assert_eq!(ring.free(), 32);
        assert_eq!(drained(&mut ring, 1024), (0, vec![]));
    }

    #[test]
    fn round_trips_records_in_order() {
        let mut state = RingState::new();
        let mut data = [0_u8; 64];
        let mut ring = Ring::new(&mut state, &mut data);
        ring.push_batch(&batch_of(&[b"one", b"", b"three"]))
            .unwrap();
        assert_eq!(ring.used(), 3 + 4 + 4 + 5 + 4);
        let (count, payloads) = drained(&mut ring, 1024);
        assert_eq!(count, 3);
        assert_eq!(payloads, vec![b"one".to_vec(), vec![], b"three".to_vec()]);
        assert!(ring.is_empty());
    }

    #[test]
    fn exact_fit_succeeds_and_one_more_byte_is_rejected() {
        let mut state = RingState::new();
        let mut data = [0_u8; 16];
        let mut ring = Ring::new(&mut state, &mut data);
        // 4 + 12 = 16 bytes: exactly the capacity.
        ring.push_batch(&batch_of(&[&[7; 12]])).unwrap();
        assert_eq!(ring.free(), 0);
        assert_eq!(
            ring.push_batch(&batch_of(&[b""])),
            Err(QueueFull { needed: 4, free: 0 })
        );
        assert_eq!(drained(&mut ring, 100).1, vec![vec![7; 12]]);

        let mut state = RingState::new();
        let mut data = [0_u8; 16];
        let mut ring = Ring::new(&mut state, &mut data);
        assert!(ring.push_batch(&batch_of(&[&[7; 13]])).is_err());
        assert!(ring.is_empty());
    }

    #[test]
    fn batch_push_is_all_or_nothing() {
        let mut state = RingState::new();
        let mut data = [0_u8; 20];
        let mut ring = Ring::new(&mut state, &mut data);
        ring.push_batch(&batch_of(&[b"aaaa"])).unwrap(); // 8 bytes
        let too_big = batch_of(&[b"bbbb", b"cccc", b"dddd"]); // 24 bytes
        assert!(ring.push_batch(&too_big).is_err());
        assert_eq!(ring.used(), 8);
        // Two of the three would fit (12 free), but nothing may be partially written.
        let two_fit_three_do_not = batch_of(&[b"bbbb", b"cccc", b"d"]);
        assert!(ring.push_batch(&two_fit_three_do_not).is_err());
        assert_eq!(drained(&mut ring, 100).1, vec![b"aaaa".to_vec()]);
    }

    #[test]
    fn records_wrap_around_the_end() {
        let mut state = RingState::new();
        let mut data = [0_u8; 20];
        let mut ring = Ring::new(&mut state, &mut data);
        // Move the positions close to the end so the next records straddle it.
        ring.push_batch(&batch_of(&[&[1; 10]])).unwrap(); // 14 bytes
        assert_eq!(drained(&mut ring, 100).0, 1);
        ring.push_batch(&batch_of(&[b"wrapped!"])).unwrap(); // 12 bytes, starts at 14
        ring.push_batch(&batch_of(&[b"x"])).unwrap(); // 5 bytes, fits after wrap
        let (_, payloads) = drained(&mut ring, 100);
        assert_eq!(payloads, vec![b"wrapped!".to_vec(), b"x".to_vec()]);
    }

    #[test]
    fn length_prefix_itself_may_wrap() {
        let mut state = RingState::new();
        let mut data = [0_u8; 10];
        let mut ring = Ring::new(&mut state, &mut data);
        ring.push_batch(&batch_of(&[&[9; 4]])).unwrap(); // 8 bytes
        drained(&mut ring, 100);
        // The next prefix starts at offset 8: 2 bytes before the end, 2 after.
        ring.push_batch(&batch_of(&[b"ab"])).unwrap();
        assert_eq!(drained(&mut ring, 100).1, vec![b"ab".to_vec()]);
    }

    #[test]
    fn drain_respects_max_bytes_but_always_makes_progress() {
        let mut state = RingState::new();
        let mut data = [0_u8; 128];
        let mut ring = Ring::new(&mut state, &mut data);
        ring.push_batch(&batch_of(&[&[1; 10], &[2; 10], &[3; 10]]))
            .unwrap();
        // Each record is 14 bytes; 20 allows one, 28 allows two.
        assert_eq!(drained(&mut ring, 20).0, 1);
        assert_eq!(drained(&mut ring, 28).0, 2);
        ring.push_batch(&batch_of(&[&[4; 50]])).unwrap();
        // A record larger than the budget is still delivered alone.
        assert_eq!(drained(&mut ring, 1).0, 1);
        assert!(ring.is_empty());
    }

    #[test]
    fn oversized_records_are_rejected_without_side_effects() {
        let mut batch = Batch::new();
        batch
            .push_record(|buf| buf.extend_from_slice(b"ok"))
            .unwrap();
        let result = batch.push_record(|buf| buf.resize(buf.len() + MAX_RECORD_BYTES + 1, 0));
        assert_eq!(
            result,
            Err(RecordTooLarge {
                len: MAX_RECORD_BYTES + 1
            })
        );
        assert_eq!(batch.records(), 1);
        assert_eq!(batch.len_bytes(), 6);
        assert!(
            batch
                .push_record(|buf| buf.resize(buf.len() + MAX_RECORD_BYTES, 0))
                .is_ok()
        );
    }

    #[test]
    fn corrupted_length_prefix_discards_the_queue() {
        let mut state = RingState::new();
        let mut data = [0_u8; 64];
        {
            let mut ring = Ring::new(&mut state, &mut data);
            ring.push_batch(&batch_of(&[b"first", b"second"])).unwrap();
        }
        data[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
        let mut ring = Ring::new(&mut state, &mut data);
        let mut out = Vec::new();
        assert!(ring.drain_into(&mut out, 1024).is_err());
        assert!(out.is_empty());
        assert!(ring.is_empty());
        // The ring is usable again afterwards.
        ring.push_batch(&batch_of(&[b"fresh"])).unwrap();
        assert_eq!(drained(&mut ring, 100).1, vec![b"fresh".to_vec()]);
    }

    #[test]
    fn length_pointing_past_the_queued_data_is_corruption() {
        let mut state = RingState::new();
        let mut data = [0_u8; 64];
        {
            let mut ring = Ring::new(&mut state, &mut data);
            ring.push_batch(&batch_of(&[b"abc"])).unwrap();
        }
        data[0..4].copy_from_slice(&100_u32.to_le_bytes());
        let mut ring = Ring::new(&mut state, &mut data);
        assert_eq!(
            ring.drain_into(&mut Vec::new(), 100),
            Err(Corrupted {
                reason: "record length out of range"
            })
        );
    }

    #[test]
    fn inconsistent_positions_are_repaired() {
        let mut state = RingState { head: 10, tail: 5 };
        let mut data = [0_u8; 16];
        let mut ring = Ring::new(&mut state, &mut data);
        assert_eq!(ring.used(), 0);
        assert!(ring.drain_into(&mut Vec::new(), 10).is_err());
        let mut state = RingState {
            head: 0,
            tail: 1_000,
        };
        let mut ring = Ring::new(&mut state, &mut data);
        ring.push_batch(&batch_of(&[b"ok"])).unwrap();
        assert_eq!(drained(&mut ring, 100).1, vec![b"ok".to_vec()]);
    }

    #[test]
    fn zero_capacity_ring_rejects_everything_and_never_panics() {
        let mut state = RingState::new();
        let mut ring = Ring::new(&mut state, &mut []);
        assert_eq!(ring.drain_into(&mut Vec::new(), 10), Ok(0));
        assert!(ring.push_batch(&batch_of(&[b""])).is_err());
        assert!(ring.push_batch(&Batch::new()).is_ok());
    }

    #[test]
    fn record_stream_validation() {
        assert_eq!(records(&[]).count(), 0);
        assert!(records(&[1, 0]).next().unwrap().is_err());
        assert!(records(&[5, 0, 0, 0, 1]).next().unwrap().is_err());
        let huge = (MAX_RECORD_BYTES as u32 + 1).to_le_bytes();
        assert!(records(&huge).next().unwrap().is_err());
        // Iteration stops after the first error.
        let mut stream = batch_of(&[b"ok"]).as_bytes().to_vec();
        stream.extend_from_slice(&[9, 9]);
        let items: Vec<_> = records(&stream).collect();
        assert_eq!(items.len(), 2);
        assert!(items[0].is_ok() && items[1].is_err());
    }

    #[test]
    fn matches_a_simple_model_over_many_wraparounds() {
        let mut state = RingState::new();
        let mut data = [0_u8; 97];
        let mut ring = Ring::new(&mut state, &mut data);
        let mut model: VecDeque<Vec<u8>> = VecDeque::new();
        let mut rng = fastrand::Rng::with_seed(42);
        let mut pushed = 0_usize;
        for round in 0..5_000_u32 {
            if rng.bool() {
                let batch_len = rng.usize(1..4);
                let payloads: Vec<Vec<u8>> = (0..batch_len)
                    .map(|_| vec![(round % 251) as u8; rng.usize(0..30)])
                    .collect();
                let refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
                let batch = batch_of(&refs);
                let fits = batch.len_bytes() <= ring.free();
                assert_eq!(ring.push_batch(&batch).is_ok(), fits);
                if fits {
                    model.extend(payloads);
                    pushed += batch_len;
                }
            } else {
                let (_, payloads) = drained(&mut ring, rng.usize(1..80));
                for payload in payloads {
                    assert_eq!(Some(payload), model.pop_front());
                }
            }
        }
        assert!(pushed > 500, "the model run should exercise many pushes");
        let (_, rest) = drained(&mut ring, usize::MAX);
        assert_eq!(rest, model.into_iter().collect::<Vec<_>>());
        assert!(state_counters_exceed_capacity(&ring));
    }

    fn state_counters_exceed_capacity(ring: &Ring) -> bool {
        ring.state.tail > 10 * ring.capacity() as u64
    }

    #[test]
    fn wake_policy() {
        // Empty before: always wake.
        assert!(should_wake(0, 10, 1000));
        // Non-empty, stays under the high-water mark: do not wake.
        assert!(!should_wake(10, 20, 1000));
        // Crosses 50%: wake.
        assert!(should_wake(400, 600, 1000));
        // Already above 50% before: the earlier push woke the consumer.
        assert!(!should_wake(600, 700, 1000));
    }
}
