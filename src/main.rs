use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use cqlprof::run::{Options, Profile, Profiler, split_statements};
use std::io::Read;
use std::path::PathBuf;
use std::process::ExitCode;

/// EXPLAIN for CQL: run statements with tracing and explain where the time and the work went.
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// `.cql` files with SELECT statements (`-` reads stdin). Use `-e` for a single statement.
    files: Vec<PathBuf>,
    /// A statement to profile.
    #[arg(short = 'e', long = "execute")]
    statement: Vec<String>,
    /// Contact point, `host:port`.
    #[arg(long, env = "CQL_NODE", default_value = "127.0.0.1:9042")]
    node: String,
    /// Rewrite every address the cluster advertises to the contact point (Docker, tunnels).
    #[arg(long)]
    single_node: bool,
    /// Keyspace to `USE`.
    #[arg(short, long)]
    keyspace: Option<String>,
    /// Untraced repetitions for the latency percentiles.
    #[arg(long, default_value_t = 50)]
    repeat: usize,
    /// Rows per page; only the first page is read.
    #[arg(long, default_value_t = 1000)]
    page_size: i32,
    /// Also run INSERT, UPDATE and DELETE statements (they change data).
    #[arg(long)]
    allow_writes: bool,
    #[arg(long, value_enum, default_value_t = Format::Text)]
    format: Format,
    /// Exit with 0 even if there are findings.
    #[arg(long)]
    exit_zero: bool,
    /// Tombstones that trigger PROF001.
    #[arg(long, default_value_t = 100)]
    max_tombstones: u64,
    /// Slow-statement threshold in milliseconds (server side).
    #[arg(long, default_value_t = 20)]
    slow_ms: i64,
}

#[derive(Clone, Copy, ValueEnum)]
enum Format {
    Text,
    Json,
}

fn main() -> ExitCode {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    match rt.block_on(run()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("cqlprof: {e:#}");
            ExitCode::from(2)
        }
    }
}

async fn run() -> Result<ExitCode> {
    let cli = Cli::parse();
    let mut statements = cli.statement.clone();
    for f in &cli.files {
        let text = if f == std::path::Path::new("-") {
            let mut s = String::new();
            std::io::stdin().read_to_string(&mut s)?;
            s
        } else {
            std::fs::read_to_string(f).with_context(|| format!("reading {}", f.display()))?
        };
        statements.extend(split_statements(&text).with_context(|| f.display().to_string())?);
    }
    if statements.is_empty() {
        bail!("nothing to profile: pass .cql files or -e '<statement>'");
    }
    let user = match (std::env::var("CQL_USER"), std::env::var("CQL_PASSWORD")) {
        (Ok(u), Ok(p)) => Some((u, p)),
        _ => None,
    };
    let th = cqlprof::analyze::Thresholds {
        tombstones: cli.max_tombstones,
        slow_us: cli.slow_ms * 1000,
        ..Default::default()
    };
    let mut prof = Profiler::connect(Options {
        node: cli.node.clone(),
        single_node: cli.single_node,
        user,
        keyspace: cli.keyspace.clone(),
        repeat: cli.repeat,
        page_size: cli.page_size,
        allow_writes: cli.allow_writes,
        thresholds: th,
    })
    .await?;
    let mut profiles = Vec::new();
    for s in &statements {
        profiles.push(prof.profile(s).await);
    }
    let findings: usize = profiles.iter().map(|p| p.findings.len()).sum();
    match cli.format {
        Format::Json => println!("{}", serde_json::to_string_pretty(&profiles)?),
        Format::Text => {
            for p in &profiles {
                print_profile(p);
            }
            let skipped = profiles.iter().filter(|p| p.skipped.is_some()).count();
            println!(
                "{} statement(s) profiled, {skipped} skipped, {findings} finding(s).",
                profiles.len() - skipped
            );
        }
    }
    Ok(if findings > 0 && !cli.exit_zero {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    })
}

fn us(v: u64) -> String {
    if v >= 1000 {
        format!("{:.2} ms", v as f64 / 1000.0)
    } else {
        format!("{v} µs")
    }
}

fn print_profile(p: &Profile) {
    let one_line = p.statement.split_whitespace().collect::<Vec<_>>().join(" ");
    println!("{one_line}");
    if let Some(s) = &p.skipped {
        println!("  skipped: {s}\n");
        return;
    }
    let l = &p.latency;
    println!(
        "  client latency   p50 {}   p90 {}   p99 {}   max {}   ({} runs)",
        us(l.p50_us),
        us(l.p90_us),
        us(l.p99_us),
        us(l.max_us),
        l.runs
    );
    if let Some(t) = &p.trace {
        let mut parts = Vec::new();
        if let Some(d) = t.duration_us {
            parts.push(format!("server {}", us(d.max(0) as u64)));
        }
        parts.push(format!("{} node(s)", t.nodes.len().max(1)));
        if let Some(r) = t.replicas_targeted {
            parts.push(format!("{r} replica(s) targeted"));
        }
        println!("  trace ({})   {}", t.engine, parts.join(", "));
        let mut work = Vec::new();
        if let Some(v) = t.live_rows {
            work.push(format!("{v} live row(s)"));
        }
        if let Some(v) = t.tombstones {
            work.push(format!("{v} tombstone(s)"));
        }
        if let Some(v) = t.sstables {
            work.push(format!("{v} sstable(s)"));
        }
        if !work.is_empty() {
            println!("  work             {}", work.join(", "));
        }
        if let Some((gap, act)) = &t.slowest_step {
            let act: String = act.chars().take(90).collect();
            println!("  slowest step     +{gap} µs  {act}");
        }
    }
    if !p.bound.is_empty() {
        println!("  bound (sampled)  {}", p.bound.join(", "));
    }
    for f in &p.findings {
        println!("  warning [{}] {}", f.code, f.message);
        println!("          {}", f.advice);
    }
    println!();
}
