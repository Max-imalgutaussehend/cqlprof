use cqlprof::analyze::{Latency, Thresholds, findings, latency};
use cqlprof::run::{is_read, split_statements, table_of};
use cqlprof::trace::{Event, summarize};

fn ev(source: &str, activity: &str, us: Option<i64>) -> Event {
    Event {
        activity: activity.into(),
        source: source.into(),
        elapsed_us: us,
    }
}

// Events as ScyllaDB 2026 and Cassandra 5.0 report them (captured with cqlsh TRACING ON).
fn scylla(live_rows: u64, dead_rows: u64) -> Vec<Event> {
    vec![
        ev("172.17.0.2", "Execute CQL3 query", Some(0)),
        ev(
            "172.17.0.2",
            "Creating read executor for token -4069959284402364209 with all: [4962c155-6eec-4f9f-8c7f-7e61ce77844b] targets: [4962c155-6eec-4f9f-8c7f-7e61ce77844b] repair decision: NONE [shard 1/sl:default]",
            Some(188),
        ),
        ev(
            "172.17.0.2",
            &format!(
                "Page stats: 1 partition(s) (1 live, 0 dead), 0 static row(s) (0 live, 0 dead), {} clustering row(s) ({live_rows} live, {dead_rows} dead), 0 range tombstone(s) and {live_rows} cell(s) ({live_rows} live, 0 dead) [shard 1/sl:default]",
                live_rows + dead_rows
            ),
            Some(317),
        ),
        ev("172.17.0.2", "Request complete", Some(331)),
    ]
}

fn cassandra(live: u64, tomb: u64, sstables: u64) -> Vec<Event> {
    vec![
        ev("172.17.0.2", "Execute CQL3 query", Some(0)),
        ev(
            "172.17.0.2",
            "Executing single-partition query on t [ReadStage-11]",
            Some(2933),
        ),
        ev(
            "172.17.0.2",
            &format!("Merged data from memtables and {sstables} sstables [ReadStage-11]"),
            Some(3311),
        ),
        ev(
            "172.17.0.2",
            &format!("Read {live} live rows and {tomb} tombstone cells [ReadStage-11]"),
            Some(3417),
        ),
        ev("172.17.0.2", "Request complete", Some(3913)),
    ]
}

#[test]
fn scylla_page_stats() {
    let s = summarize(&scylla(10, 2990), Some(833), Some("172.17.0.2".into()));
    assert_eq!(s.engine, "scylla");
    assert_eq!(
        (s.live_rows, s.tombstones, s.replicas_targeted),
        (Some(10), Some(2990), Some(1))
    );
    assert_eq!(s.nodes, ["172.17.0.2"]);
    assert_eq!(s.slowest_step.unwrap().0, 188); // 0 -> 188 is the largest gap
}

#[test]
fn cassandra_read_line() {
    let s = summarize(&cassandra(3, 5000, 9), Some(3913), None);
    assert_eq!(s.engine, "cassandra");
    assert_eq!(
        (s.live_rows, s.tombstones, s.sstables),
        (Some(3), Some(5000), Some(9))
    );
}

#[test]
fn multi_node_traces_count_each_nodes_work() {
    let mut e = cassandra(2, 0, 1);
    e.push(ev(
        "10.0.0.2",
        "Read 4 live rows and 1 tombstone cells [ReadStage-2]",
        Some(900),
    ));
    let s = summarize(&e, None, None);
    assert_eq!(s.live_rows, Some(6));
    assert_eq!(s.nodes.len(), 2);
}

#[test]
fn findings_fire_on_thresholds() {
    let th = Thresholds::default();
    let lat = Latency::default();
    let f = findings(&summarize(&scylla(10, 2990), Some(800), None), &lat, &th);
    assert_eq!(f.iter().map(|f| f.code).collect::<Vec<_>>(), ["PROF001"]);
    assert!(findings(&summarize(&scylla(10, 5), Some(800), None), &lat, &th).is_empty());
    let f = findings(
        &summarize(&cassandra(1, 0, 12), Some(30_000), None),
        &lat,
        &th,
    );
    assert_eq!(
        f.iter().map(|f| f.code).collect::<Vec<_>>(),
        ["PROF002", "PROF004"]
    );
    let f = findings(
        &summarize(&cassandra(5000, 0, 1), Some(800), None),
        &lat,
        &th,
    );
    assert_eq!(f[0].code, "PROF003");
}

#[test]
fn tombstones_below_live_rows_are_fine() {
    // lots of tombstones but even more live rows: not a tombstone-scan problem
    assert!(
        findings(
            &summarize(&cassandra(900, 200, 1), None, None),
            &Latency::default(),
            &Thresholds::default()
        )
        .is_empty()
    );
}

#[test]
fn latency_percentiles_and_tail_finding() {
    let mut v = vec![100u64; 95];
    v.extend([2000; 5]);
    let l = latency(v);
    assert_eq!((l.runs, l.p50_us, l.max_us), (100, 100, 2000));
    assert!(l.p99_us >= 2000);
    let f = findings(
        &summarize(&cassandra(1, 0, 1), None, None),
        &l,
        &Thresholds::default(),
    );
    assert_eq!(f[0].code, "PROF005");
    assert_eq!(latency(vec![]).runs, 0);
}

#[test]
fn statements_tables_and_kinds() {
    let s = split_statements("-- c\nselect a from ks.t where x = 1;\nbegin batch insert into t (a) values (1); apply batch;\nSELECT 1 FROM \"My\".\"T\"").unwrap();
    assert_eq!(s.len(), 3);
    assert_eq!(table_of(&s[0]).as_deref(), Some("ks.t"));
    assert_eq!(
        table_of("INSERT INTO t (a) VALUES (1)").as_deref(),
        Some("t")
    );
    assert_eq!(
        table_of("UPDATE ks.t SET a = 1 WHERE b = 2").as_deref(),
        Some("ks.t")
    );
    assert!(is_read(&s[0]) && !is_read(&s[1]));
}
