//! Turns a tracing session of Apache Cassandra or ScyllaDB into numbers.

use regex::Regex;
use serde::Serialize;
use std::collections::BTreeSet;
use std::sync::LazyLock;

#[derive(Clone, Debug)]
pub struct Event {
    pub activity: String,
    pub source: String,
    /// Microseconds since the request started on that node, when the server reports it.
    pub elapsed_us: Option<i64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct TraceSummary {
    /// `cassandra`, `scylla` or `unknown`, guessed from the wording of the events.
    pub engine: String,
    pub duration_us: Option<i64>,
    pub coordinator: Option<String>,
    /// Nodes that logged events (the coordinator and the replicas that did work).
    pub nodes: Vec<String>,
    pub replicas_targeted: Option<usize>,
    pub live_rows: Option<u64>,
    pub tombstones: Option<u64>,
    pub sstables: Option<u64>,
    pub partitions: Option<u64>,
    pub events: usize,
    /// The step that took longest: (microseconds since the previous event, activity).
    pub slowest_step: Option<(i64, String)>,
    pub used_paxos: bool,
}

static C_READ: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"Read (\d+) live rows? and (\d+) tombstone cells?").unwrap());
static C_MERGE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"Merged? data from memtables and (\d+) sstables?").unwrap());
static S_PAGE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"Page stats: (\d+) partition\(s\) \((\d+) live, (\d+) dead\), \d+ static row\(s\) \((\d+) live, (\d+) dead\), \d+ clustering row\(s\) \((\d+) live, (\d+) dead\), (\d+) range tombstone\(s\) and \d+ cell\(s\) \((\d+) live, (\d+) dead\)").unwrap()
});
static S_TARGETS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"targets: \[([^\]]*)\]").unwrap());
static S_SSTABLES: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:sstables?|SSTables?)[^0-9]{0,40}(\d+)").unwrap());

/// Summarises the events of one tracing session. `duration_us` is the session's reported duration.
pub fn summarize(
    events: &[Event],
    duration_us: Option<i64>,
    coordinator: Option<String>,
) -> TraceSummary {
    let mut s = TraceSummary {
        duration_us,
        coordinator,
        events: events.len(),
        engine: "unknown".into(),
        ..Default::default()
    };
    let mut nodes = BTreeSet::new();
    let (mut live, mut tomb, mut sst, mut parts) =
        (None::<u64>, None::<u64>, None::<u64>, None::<u64>);
    let mut targets = None::<usize>;
    for e in events {
        nodes.insert(e.source.clone());
        let a = e.activity.as_str();
        if let Some(c) = C_READ.captures(a) {
            s.engine = "cassandra".into();
            live = Some(live.unwrap_or(0) + c[1].parse::<u64>().unwrap_or(0));
            tomb = Some(tomb.unwrap_or(0) + c[2].parse::<u64>().unwrap_or(0));
        }
        if let Some(c) = C_MERGE.captures(a) {
            s.engine = "cassandra".into();
            sst = Some(sst.unwrap_or(0) + c[1].parse::<u64>().unwrap_or(0));
        }
        if let Some(c) = S_PAGE.captures(a) {
            s.engine = "scylla".into();
            let n = |i: usize| c[i].parse::<u64>().unwrap_or(0);
            parts = Some(parts.unwrap_or(0) + n(1));
            // live clustering rows (+ static) are the result; dead rows, range tombstones and dead cells are tombstones
            live = Some(live.unwrap_or(0) + n(6));
            tomb = Some(tomb.unwrap_or(0) + n(7) + n(5) + n(8) + n(10) + n(3));
        }
        if let Some(c) = S_TARGETS.captures(a) {
            s.engine = "scylla".into();
            let n = c[1].split(',').filter(|x| !x.trim().is_empty()).count();
            targets = Some(targets.map_or(n, |t| t.max(n)));
        }
        if a.contains("Paxos") || a.contains("paxos") {
            s.used_paxos = true;
        }
        if s.engine == "scylla"
            && sst.is_none()
            && let Some(c) = S_SSTABLES.captures(a)
        {
            sst = c[1].parse().ok();
        }
    }
    s.nodes = nodes.into_iter().collect();
    s.live_rows = live;
    s.tombstones = tomb;
    s.sstables = sst;
    s.partitions = parts;
    s.replicas_targeted = targets;
    // slowest step: largest increase of source_elapsed between consecutive events of one node
    let mut by_node: std::collections::BTreeMap<&str, i64> = Default::default();
    let mut best: Option<(i64, String)> = None;
    for e in events {
        let Some(t) = e.elapsed_us else { continue };
        let prev = by_node.insert(e.source.as_str(), t).unwrap_or(0);
        let gap = t - prev;
        if gap > 0 && best.as_ref().is_none_or(|(g, _)| gap > *g) {
            best = Some((gap, e.activity.clone()));
        }
    }
    s.slowest_step = best;
    s
}
