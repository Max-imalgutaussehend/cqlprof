//! Runs the binary against a real node named by `CQLPROF_TEST_NODE` (CI starts ScyllaDB and Cassandra 5.0).
//! `cargo test --test integration -- --ignored`

use std::process::{Command, Output};

fn node() -> String {
    std::env::var("CQLPROF_TEST_NODE").expect("set CQLPROF_TEST_NODE=host:port")
}

fn sh(stmt: &str) {
    let c = std::env::var("CQLPROF_CONTAINER").unwrap_or_else(|_| "db".into());
    let o = Command::new("docker")
        .args(["exec", &c, "cqlsh", "-e", stmt])
        .output()
        .unwrap();
    assert!(
        o.status.success(),
        "cqlsh failed: {}{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    );
}

fn prof(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cqlprof"))
        .args(["--node", &node(), "--single-node", "--repeat", "20"])
        .args(args)
        .output()
        .unwrap()
}

fn text(o: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    )
}

#[test]
#[ignore = "needs a database (CQLPROF_TEST_NODE)"]
fn finds_tombstones_and_clean_reads() {
    sh(
        "CREATE KEYSPACE IF NOT EXISTS prof_it WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}",
    );
    sh("DROP TABLE IF EXISTS prof_it.q");
    sh(
        "CREATE TABLE prof_it.q (pk int, ck int, v text, PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000",
    );
    // partition 1: 1,500 deleted rows and 5 live rows; partition 2: 5 live rows
    let mut script = String::from("BEGIN UNLOGGED BATCH ");
    for i in 0..1500 {
        script.push_str(&format!(
            "INSERT INTO prof_it.q (pk, ck, v) VALUES (1, {i}, 'x'); "
        ));
    }
    script.push_str("APPLY BATCH");
    sh(&script);
    let mut del = String::from("BEGIN UNLOGGED BATCH ");
    for i in 0..1495 {
        del.push_str(&format!(
            "DELETE FROM prof_it.q WHERE pk = 1 AND ck = {i}; "
        ));
    }
    del.push_str("APPLY BATCH");
    sh(&del);
    sh("INSERT INTO prof_it.q (pk, ck, v) VALUES (2, 1, 'a')");

    let bad = prof(&["-e", "SELECT * FROM prof_it.q WHERE pk = 1"]);
    let t = text(&bad);
    assert_eq!(bad.status.code(), Some(1), "{t}");
    assert!(t.contains("PROF001") && t.contains("tombstone"), "{t}");

    let good = prof(&["-e", "SELECT * FROM prof_it.q WHERE pk = 2"]);
    assert_eq!(good.status.code(), Some(0), "{}", text(&good));

    // `?` markers get values from an existing row; writes are skipped unless allowed
    let q = prof(&[
        "-e",
        "SELECT * FROM prof_it.q WHERE pk = ?",
        "-e",
        "INSERT INTO prof_it.q (pk, ck, v) VALUES (9, 9, 'z')",
        "--exit-zero",
    ]);
    let t = text(&q);
    assert!(t.contains("bound (sampled)"), "{t}");
    assert!(t.contains("skipped: not a SELECT"), "{t}");

    let j = prof(&[
        "--format",
        "json",
        "--exit-zero",
        "-e",
        "SELECT * FROM prof_it.q WHERE pk = 1",
    ]);
    let v: serde_json::Value = serde_json::from_slice(&j.stdout).unwrap();
    assert_eq!(v[0]["findings"][0]["code"], "PROF001");
    assert!(v[0]["trace"]["tombstones"].as_u64().unwrap() >= 1495);
    sh("DROP KEYSPACE IF EXISTS prof_it");
}
