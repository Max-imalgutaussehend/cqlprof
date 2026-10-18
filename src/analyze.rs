//! Findings from one tracing session plus the latency distribution of repeated runs.

use crate::trace::TraceSummary;
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct Finding {
    pub code: &'static str,
    pub name: &'static str,
    pub message: String,
    pub advice: &'static str,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Latency {
    pub runs: usize,
    pub min_us: u64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
}

pub fn latency(mut samples_us: Vec<u64>) -> Latency {
    if samples_us.is_empty() {
        return Latency::default();
    }
    samples_us.sort_unstable();
    let q = |p: f64| samples_us[(((samples_us.len() - 1) as f64) * p).round() as usize];
    Latency {
        runs: samples_us.len(),
        min_us: samples_us[0],
        p50_us: q(0.5),
        p90_us: q(0.9),
        p99_us: q(0.99),
        max_us: *samples_us.last().unwrap(),
    }
}

#[derive(Clone, Debug)]
pub struct Thresholds {
    pub tombstones: u64,
    pub sstables: u64,
    pub page_rows: u64,
    pub slow_us: i64,
    pub tail_ratio: f64,
}

impl Default for Thresholds {
    fn default() -> Self {
        Thresholds {
            tombstones: 100,
            sstables: 8,
            page_rows: 1000,
            slow_us: 20_000,
            tail_ratio: 5.0,
        }
    }
}

pub fn findings(t: &TraceSummary, lat: &Latency, th: &Thresholds) -> Vec<Finding> {
    let mut out = Vec::new();
    if let (Some(tomb), live) = (t.tombstones, t.live_rows.unwrap_or(0))
        && tomb >= th.tombstones
        && tomb > live
    {
        out.push(Finding {
            code: "PROF001",
            name: "tombstone-scan",
            message: format!("the read scanned {tomb} tombstones to return {live} live row(s)"),
            advice: "Deleted data is read again on every query until it is compacted away (gc_grace_seconds). Avoid delete-then-read queues, use TTL with time-window compaction, or model the data so deletes drop whole partitions.",
        });
    }
    if let Some(n) = t.sstables
        && n >= th.sstables
    {
        out.push(Finding {
            code: "PROF002",
            name: "many-sstables",
            message: format!("the read merged {n} sstables"),
            advice: "Every sstable adds disk reads. Check that compaction keeps up, that the compaction strategy fits the access pattern, and that the partition is not rewritten constantly.",
        });
    }
    if let Some(rows) = t.live_rows
        && rows >= th.page_rows
    {
        out.push(Finding {
            code: "PROF003",
            name: "large-read",
            message: format!("the first page returned {rows} rows"),
            advice: "Large reads move a lot of data through one coordinator. Add a LIMIT, narrow the clustering range, or select fewer columns.",
        });
    }
    if let Some(d) = t.duration_us
        && d >= th.slow_us
    {
        out.push(Finding {
            code: "PROF004",
            name: "slow-statement",
            message: format!("the server needed {:.1} ms{}", d as f64 / 1000.0, t.slowest_step.as_ref().map_or(String::new(), |(g, a)| format!("; slowest step: {a} (+{g} µs)"))),
            advice: "Look at the slowest step in the trace: cache misses and disk reads, tombstones, or a coordinator waiting for a remote replica.",
        });
    }
    if lat.runs >= 20 && lat.p50_us > 0 && lat.p99_us as f64 > th.tail_ratio * lat.p50_us as f64 {
        out.push(Finding {
            code: "PROF005",
            name: "tail-latency",
            message: format!("p99 is {:.1}x the median ({} µs vs {} µs)", lat.p99_us as f64 / lat.p50_us as f64, lat.p99_us, lat.p50_us),
            advice: "A long tail with a fast median usually means compaction or GC pauses, cold cache or a hot partition. Repeat the run under load.",
        });
    }
    if t.used_paxos {
        out.push(Finding {
            code: "PROF006",
            name: "lightweight-transaction",
            message: "the statement ran a Paxos round".into(),
            advice: "Lightweight transactions need several round trips and serialise per partition. Use them only where you need compare-and-set.",
        });
    }
    out
}
