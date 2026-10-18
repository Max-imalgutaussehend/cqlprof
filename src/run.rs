//! Connects, prepares, binds sample values, executes with and without tracing.

use crate::analyze::{Finding, Latency, Thresholds, findings, latency};
use crate::trace::{Event, TraceSummary, summarize};
use anyhow::{Context, Result, bail};
use cqlc_core::lexer::{TokenKind as K, lex};
use cqlc_core::span::FileId;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::TranslationError;
use scylla::policies::address_translator::{AddressTranslator, UntranslatedPeer};
use scylla::response::PagingState;
use scylla::value::{CqlValue, Row};
use serde::Serialize;
use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Instant;

struct SingleNode(SocketAddr);

#[async_trait::async_trait]
impl AddressTranslator for SingleNode {
    async fn translate_address(
        &self,
        _: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        Ok(self.0)
    }
}

pub struct Options {
    pub node: String,
    pub single_node: bool,
    pub user: Option<(String, String)>,
    pub keyspace: Option<String>,
    pub repeat: usize,
    pub page_size: i32,
    pub allow_writes: bool,
    pub thresholds: Thresholds,
}

#[derive(Debug, Serialize)]
pub struct Profile {
    pub statement: String,
    pub skipped: Option<String>,
    pub latency: Latency,
    pub trace: Option<TraceSummary>,
    pub findings: Vec<Finding>,
    /// Values bound to `?` markers, as the sampler chose them.
    pub bound: Vec<String>,
}

/// Statements of a file (`;`-separated; a `BEGIN ... APPLY BATCH` block is one statement).
pub fn split_statements(text: &str) -> Result<Vec<String>> {
    let (lexed, diags) = lex(FileId(0), text);
    if let Some(d) = diags.iter().find(|d| d.is_error()) {
        bail!("{}", d.message);
    }
    let mut out = Vec::new();
    let (mut start, mut in_batch, mut prev_apply, mut first) = (None::<usize>, false, false, true);
    for t in lexed.tokens.iter().filter(|t| t.kind != K::Eof) {
        let r = t.span.range();
        let kw = if let K::Ident(i) = &t.kind {
            i.to_ascii_uppercase()
        } else {
            String::new()
        };
        if start.is_none() {
            start = Some(r.start);
            first = true;
        }
        if first && kw == "BEGIN" {
            in_batch = true;
        }
        first = false;
        if in_batch && prev_apply && kw == "BATCH" {
            in_batch = false;
        }
        prev_apply = kw == "APPLY";
        if t.kind == K::Semi && !in_batch {
            let s = start.take().unwrap();
            let stmt = text[s..r.start].trim();
            if !stmt.is_empty() {
                out.push(stmt.to_string());
            }
        }
    }
    if let Some(s) = start {
        let stmt = text[s..].trim();
        if !stmt.is_empty() {
            out.push(stmt.to_string());
        }
    }
    Ok(out)
}

/// The table a statement reads or writes: the name after `FROM`, `INTO` or `UPDATE`.
pub fn table_of(stmt: &str) -> Option<String> {
    let (lexed, _) = lex(FileId(0), stmt);
    let toks: Vec<_> = lexed.tokens.iter().filter(|t| t.kind != K::Eof).collect();
    for (i, t) in toks.iter().enumerate() {
        if let K::Ident(w) = &t.kind
            && matches!(w.to_ascii_uppercase().as_str(), "FROM" | "INTO" | "UPDATE")
        {
            let mut name = String::new();
            let mut j = i + 1;
            while j < toks.len() {
                match &toks[j].kind {
                    K::Ident(s) | K::QuotedIdent(s) => name.push_str(s),
                    K::Dot => name.push('.'),
                    _ => break,
                }
                if !matches!(toks.get(j + 1).map(|t| &t.kind), Some(K::Dot)) && !name.ends_with('.')
                {
                    break;
                }
                j += 1;
            }
            return Some(name);
        }
    }
    None
}

pub fn is_read(stmt: &str) -> bool {
    stmt.split_whitespace()
        .next()
        .is_some_and(|w| w.eq_ignore_ascii_case("select"))
}

pub struct Profiler {
    session: Session,
    opts: Options,
    samples: HashMap<String, Option<HashMap<String, CqlValue>>>,
}

impl Profiler {
    pub async fn connect(opts: Options) -> Result<Profiler> {
        let addr = opts
            .node
            .to_socket_addrs()
            .with_context(|| format!("resolving {}", opts.node))?
            .next()
            .with_context(|| format!("{} did not resolve", opts.node))?;
        let mut b = SessionBuilder::new().known_node_addr(addr);
        if opts.single_node {
            b = b.address_translator(Arc::new(SingleNode(addr)));
        }
        if let Some((u, p)) = &opts.user {
            b = b.user(u, p);
        }
        if let Some(ks) = &opts.keyspace {
            b = b.use_keyspace(ks, false);
        }
        let session = b
            .build()
            .await
            .with_context(|| format!("connecting to {}", opts.node))?;
        Ok(Profiler {
            session,
            opts,
            samples: HashMap::new(),
        })
    }

    /// One existing row of `table`, used to give `?` markers realistic values.
    async fn sample(&mut self, table: &str) -> Option<HashMap<String, CqlValue>> {
        if let Some(s) = self.samples.get(table) {
            return s.clone();
        }
        let got = async {
            let res = self
                .session
                .query_unpaged(format!("SELECT * FROM {table} LIMIT 1"), &[])
                .await
                .ok()?;
            let rows = res.into_rows_result().ok()?;
            let row = rows.first_row::<Row>().ok()?;
            let mut m = HashMap::new();
            for (spec, v) in rows.column_specs().iter().zip(row.columns) {
                if let Some(v) = v {
                    m.insert(spec.name().to_ascii_lowercase(), v);
                }
            }
            Some(m)
        }
        .await;
        self.samples.insert(table.to_string(), got.clone());
        got
    }

    pub async fn profile(&mut self, stmt: &str) -> Profile {
        let mut p = Profile {
            statement: stmt.to_string(),
            skipped: None,
            latency: Latency::default(),
            trace: None,
            findings: vec![],
            bound: vec![],
        };
        if !is_read(stmt) && !self.opts.allow_writes {
            p.skipped = Some("not a SELECT: running it would change data (pass --allow-writes to profile writes)".into());
            return p;
        }
        match self.run(stmt, &mut p).await {
            Ok(()) => {}
            Err(e) => p.skipped = Some(format!("{e:#}")),
        }
        p
    }

    async fn run(&mut self, stmt: &str, p: &mut Profile) -> Result<()> {
        let mut prepared = self
            .session
            .prepare(stmt)
            .await
            .context("the server rejected the statement")?;
        prepared.set_page_size(self.opts.page_size);
        let specs: Vec<(String, String)> = prepared
            .get_variable_col_specs()
            .iter()
            .map(|c| (c.name().to_ascii_lowercase(), format!("{:?}", c.typ())))
            .collect();
        let mut values: Vec<CqlValue> = Vec::new();
        if !specs.is_empty() {
            let table = table_of(stmt).context("cannot find the table to sample values from")?;
            let sample = self.sample(&table).await.with_context(|| format!("cannot sample values: {table} is empty or unreadable (load representative data first)"))?;
            for (name, ty) in &specs {
                let v = match name.as_str() {
                    "[limit]" | "[per_partition_limit]" => CqlValue::Int(10),
                    "[ttl]" => CqlValue::Int(0),
                    "[timestamp]" => CqlValue::BigInt(0),
                    n => sample.get(n).cloned().with_context(|| {
                        format!("no sample value for `{n}` ({ty}); put a literal in the statement")
                    })?,
                };
                p.bound.push(format!("{name} = {v:?}"));
                values.push(v);
            }
        }
        // latency: untraced repetitions (the first one warms the caches and is dropped)
        let mut samples = Vec::new();
        for i in 0..=self.opts.repeat {
            let t = Instant::now();
            self.session
                .execute_single_page(&prepared, &values, PagingState::start())
                .await?;
            if i > 0 {
                samples.push(t.elapsed().as_micros() as u64);
            }
        }
        p.latency = latency(samples);
        // one traced run
        prepared.set_tracing(true);
        let (res, _) = self
            .session
            .execute_single_page(&prepared, &values, PagingState::start())
            .await?;
        if let Some(id) = res.tracing_id() {
            let info = self
                .session
                .get_tracing_info(&id)
                .await
                .context("reading the tracing session")?;
            let events: Vec<Event> = info
                .events
                .iter()
                .map(|e| Event {
                    activity: e.activity.clone().unwrap_or_default(),
                    source: e.source.map(|s| s.to_string()).unwrap_or_default(),
                    elapsed_us: e.source_elapsed.map(i64::from),
                })
                .collect();
            let t = summarize(
                &events,
                info.duration.map(i64::from),
                info.coordinator.map(|c| c.to_string()),
            );
            p.findings = findings(&t, &p.latency, &self.opts.thresholds);
            p.trace = Some(t);
        }
        Ok(())
    }
}
