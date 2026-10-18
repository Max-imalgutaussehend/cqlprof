# cqlprof

**`EXPLAIN` for CQL.** Cassandra and ScyllaDB have no query planner output, but they have tracing. `cqlprof` runs your `SELECT`
statements with tracing on, reads the trace and tells you where the time and the work went. Part of the
[cqlz family](https://max-imalgutaussehend.github.io/cqlz/cqlprof.html).

```sh
cargo install cqlprof

cqlprof -e "SELECT * FROM shop.orders WHERE customer = 1"
cqlprof queries/*.cql -k shop --repeat 100
cqlprof queries/orders.cql --format json --exit-zero      # for dashboards and scripts
```

```
SELECT * FROM pf.q WHERE pk = 1
  client latency   p50 1.11 ms   p90 1.59 ms   p99 1.95 ms   max 1.95 ms   (30 runs)
  trace (scylla)   server 833 µs, 1 node(s), 1 replica(s) targeted
  work             10 live row(s), 2990 tombstone(s)
  slowest step     +743 µs  Page stats: 1 partition(s) (1 live, 0 dead), 0 static row(s) ...
  warning [PROF001] the read scanned 2990 tombstones to return 10 live row(s)
          Deleted data is read again on every query until it is compacted away ...
```

## What it measures

For every statement: client-side latency percentiles over `--repeat` runs (the first run warms caches and is dropped), and one
traced run that is parsed for the work the server did: live rows, **tombstones**, **sstables** merged, replicas targeted, nodes
involved, the slowest step, and whether a Paxos round ran. It understands the trace wording of **Apache Cassandra 4/5** and
**ScyllaDB**. Only the first page (`--page-size`, default 1000 rows) is read, so a missing `LIMIT` cannot scan a whole table.

| Code | Name | Fires when |
|---|---|---|
| PROF001 | tombstone-scan | tombstones scanned >= 100 and more than the live rows returned |
| PROF002 | many-sstables | 8 or more sstables merged (Cassandra) |
| PROF003 | large-read | the first page returned 1000 rows |
| PROF004 | slow-statement | the server needed more than `--slow-ms` (20 ms) |
| PROF005 | tail-latency | p99 is more than 5 times the median (20+ runs) |
| PROF006 | lightweight-transaction | the statement ran a Paxos round |

Exit code 1 when there are findings (`--exit-zero` to disable), 2 on errors.

## Bind markers

A statement with `?` markers is prepared on the server; each marker gets its value from **one existing row of the table**
(`SELECT * FROM t LIMIT 1`), matched by column name; `LIMIT ?` gets 10. The values are printed (`bound (sampled)`) so you can judge
whether they are representative. If the table is empty or a marker cannot be filled, the statement is skipped with the reason: load
representative data first, or put a literal in the statement.

## Safety

Only `SELECT` is executed. `INSERT`, `UPDATE`, `DELETE` and batches are skipped unless you pass `--allow-writes`.

## Options

`--node` (or `CQL_NODE`), `--single-node` (Docker, tunnels), `-k/--keyspace`, `--repeat`, `--page-size`, `--max-tombstones`,
`--slow-ms`, `--format text|json`. Credentials: `CQL_USER` and `CQL_PASSWORD`.

## Honest limits

Tracing adds overhead and one traced run is a sample, not a benchmark; the latency percentiles come from the untraced runs.
Sampled bind values hit one partition, so a hot or cold partition can mislead. Trace wording is not a stable API: engines that
report differently simply produce fewer numbers. For model-based predictions before you have a cluster see
[cql-ai](https://github.com/Max-imalgutaussehend/cql-ai).

## Tested

Parsing of real Cassandra 5.0 and ScyllaDB traces, the findings and the percentile code are unit tested; CI runs the binary against
ScyllaDB and Cassandra 5.0 with a table full of tombstones.

Apache-2.0
