use std::cmp::max;
use std::env;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use jetgraph_client::{Client, NodeRef};

#[derive(Clone, Copy, Debug)]
enum Mode {
    Upsert,
    Query,
    Both,
}

impl Mode {
    fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "upsert" => Ok(Self::Upsert),
            "query" => Ok(Self::Query),
            "both" => Ok(Self::Both),
            other => Err(format!("invalid mode '{other}', expected upsert|query|both")),
        }
    }
}

#[derive(Clone, Debug)]
struct Config {
    endpoint: String,
    /// Create one independent gRPC connection per worker instead of sharing one.
    /// Removes h2 connection-level mutex contention under high concurrency.
    per_worker_connections: bool,
    mode: Mode,
    edge_type: String,
    src_type: String,
    dst_type: String,
    src_prefix: String,
    dst_prefix: String,
    pair_count: usize,
    upsert_requests: usize,
    query_requests: usize,
    warmup_requests: usize,
    concurrency: usize,
    event_span_secs: u32,
    bootstrap_schema: bool,
    use_node_ids: bool,
    require_sub_ms: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:50051".to_string(),
            per_worker_connections: false,
            mode: Mode::Both,
            edge_type: "TRANSACTS_AT".to_string(),
            src_type: "card".to_string(),
            dst_type: "merchant".to_string(),
            src_prefix: "load-card".to_string(),
            dst_prefix: "load-merchant".to_string(),
            pair_count: 20_000,
            upsert_requests: 200_000,
            query_requests: 200_000,
            warmup_requests: 10_000,
            concurrency: 64,
            event_span_secs: 86_400,
            bootstrap_schema: false,
            use_node_ids: true,
            require_sub_ms: false,
        }
    }
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut cfg = Self::default();
        let args: Vec<String> = env::args().skip(1).collect();
        let mut i = 0usize;
        while i < args.len() {
            match args[i].as_str() {
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                "--endpoint" => {
                    i += 1;
                    cfg.endpoint = value(&args, i, "--endpoint")?;
                }
                "--mode" => {
                    i += 1;
                    cfg.mode = Mode::parse(&value(&args, i, "--mode")?)?;
                }
                "--edge-type" => {
                    i += 1;
                    cfg.edge_type = value(&args, i, "--edge-type")?;
                }
                "--src-type" => {
                    i += 1;
                    cfg.src_type = value(&args, i, "--src-type")?;
                }
                "--dst-type" => {
                    i += 1;
                    cfg.dst_type = value(&args, i, "--dst-type")?;
                }
                "--src-prefix" => {
                    i += 1;
                    cfg.src_prefix = value(&args, i, "--src-prefix")?;
                }
                "--dst-prefix" => {
                    i += 1;
                    cfg.dst_prefix = value(&args, i, "--dst-prefix")?;
                }
                "--pair-count" => {
                    i += 1;
                    cfg.pair_count = parse_usize(&value(&args, i, "--pair-count")?, "--pair-count")?;
                }
                "--upsert-requests" => {
                    i += 1;
                    cfg.upsert_requests = parse_usize(&value(&args, i, "--upsert-requests")?, "--upsert-requests")?;
                }
                "--query-requests" => {
                    i += 1;
                    cfg.query_requests = parse_usize(&value(&args, i, "--query-requests")?, "--query-requests")?;
                }
                "--warmup-requests" => {
                    i += 1;
                    cfg.warmup_requests = parse_usize(&value(&args, i, "--warmup-requests")?, "--warmup-requests")?;
                }
                "--concurrency" => {
                    i += 1;
                    cfg.concurrency = parse_usize(&value(&args, i, "--concurrency")?, "--concurrency")?;
                }
                "--event-span-secs" => {
                    i += 1;
                    cfg.event_span_secs = parse_u32(&value(&args, i, "--event-span-secs")?, "--event-span-secs")?;
                }
                "--bootstrap-schema" => {
                    cfg.bootstrap_schema = true;
                }
                "--use-external-refs" => {
                    cfg.use_node_ids = false;
                }
                "--require-sub-ms" => {
                    cfg.require_sub_ms = true;
                }
                "--per-worker-connections" => {
                    cfg.per_worker_connections = true;
                }
                flag => {
                    return Err(format!("unknown argument '{flag}' (use --help)"));
                }
            }
            i += 1;
        }

        if cfg.pair_count == 0 {
            return Err("--pair-count must be greater than 0".to_string());
        }
        if cfg.concurrency == 0 {
            return Err("--concurrency must be greater than 0".to_string());
        }

        Ok(cfg)
    }
}

#[derive(Default, Debug)]
struct WorkerStats {
    ok_count: usize,
    err_count: usize,
    miss_count: usize,
    latencies_micros: Vec<u64>,
}

#[derive(Debug)]
struct RunSummary {
    name: &'static str,
    elapsed: Duration,
    ok_count: usize,
    err_count: usize,
    miss_count: usize,
    latencies_micros: Vec<u64>,
}

#[derive(Clone)]
struct WorkloadData {
    pairs: Arc<Vec<(NodeRef, NodeRef)>>,
}

/// Create a pool of clients: one per worker if `--per-worker-connections` is set,
/// otherwise N clones of a single shared channel.
async fn make_clients(cfg: &Config) -> Result<Vec<Client>, Box<dyn Error>> {
    if cfg.per_worker_connections {
        println!("Creating {} independent connections (one per worker)...", cfg.concurrency);
        let mut clients = Vec::with_capacity(cfg.concurrency);
        for _ in 0..cfg.concurrency {
            clients.push(Client::connect(&cfg.endpoint).await?);
        }
        Ok(clients)
    } else {
        let c = Client::connect(&cfg.endpoint).await?;
        Ok(vec![c; cfg.concurrency])
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cfg = Config::parse().map_err(|e| format!("argument error: {e}"))?;
    println!("gRPC load test config: {cfg:#?}");

    let client = Client::connect(&cfg.endpoint).await?;
    let healthy = client.health().check().await?;
    if !healthy {
        return Err("engine is not ready (health=NOT_SERVING)".into());
    }
    println!("Engine health: READY");

    let data = prepare_workload_data(&client, &cfg).await?;
    seed_edges_for_queries(&client, &cfg, &data).await?;

    if matches!(cfg.mode, Mode::Upsert | Mode::Both) {
        warmup_upsert(&client, &cfg, &data).await?;
        let clients = make_clients(&cfg).await?;
        let summary = run_upsert_load(clients, cfg.clone(), data.clone()).await?;
        print_summary(&summary);
        enforce_sub_ms_if_requested(&cfg, &summary)?;
    }

    if matches!(cfg.mode, Mode::Query | Mode::Both) {
        warmup_query(&client, &cfg, &data).await?;
        let clients = make_clients(&cfg).await?;
        let summary = run_query_load(clients, cfg.clone(), data.clone()).await?;
        print_summary(&summary);
        enforce_sub_ms_if_requested(&cfg, &summary)?;
    }

    Ok(())
}

fn print_help() {
    println!(
        "Usage: cargo run --example grpc_load_test -- [options]

Options:
  --endpoint <url>             gRPC endpoint (default: http://localhost:50051)
  --mode <upsert|query|both>   Which workload to execute (default: both)
  --edge-type <name>           Edge type to test (default: TRANSACTS_AT)
  --src-type <name>            Source node type (default: card)
  --dst-type <name>            Destination node type (default: merchant)
  --src-prefix <prefix>        Source external id prefix (default: load-card)
  --dst-prefix <prefix>        Destination external id prefix (default: load-merchant)
  --pair-count <n>             Number of reusable src/dst pairs (default: 20000)
  --upsert-requests <n>        Number of measured upsert requests (default: 200000)
  --query-requests <n>         Number of measured query requests (default: 200000)
  --warmup-requests <n>        Warmup requests per workload (default: 10000)
  --concurrency <n>            Number of concurrent worker tasks (default: 64)
  --event-span-secs <n>        Randomized upsert event timestamp span (default: 86400)
  --bootstrap-schema           Register src/dst node types + compact edge type when missing
  --use-external-refs          Use external refs instead of node_id fast path
  --require-sub-ms             Fail run if lat_avg_ms is >= 1.000
"
    );
}

fn value(args: &[String], idx: usize, flag: &str) -> Result<String, String> {
    args.get(idx)
        .cloned()
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn parse_usize(raw: &str, flag: &str) -> Result<usize, String> {
    raw.parse::<usize>()
        .map_err(|_| format!("invalid usize for {flag}: '{raw}'"))
}

fn parse_u32(raw: &str, flag: &str) -> Result<u32, String> {
    raw.parse::<u32>()
        .map_err(|_| format!("invalid u32 for {flag}: '{raw}'"))
}

fn now_secs() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs() as u32
}

fn pair_for(req_idx: usize, cfg: &Config, pairs: &[(NodeRef, NodeRef)]) -> (NodeRef, NodeRef) {
    let src_i = req_idx % cfg.pair_count;
    let dst_i = (req_idx.wrapping_mul(31).wrapping_add(7)) % cfg.pair_count;
    (pairs[src_i].0.clone(), pairs[dst_i].1.clone())
}

async fn prepare_workload_data(client: &Client, cfg: &Config) -> Result<WorkloadData, Box<dyn Error>> {
    ensure_schema(client, cfg).await?;

    println!("Ensuring node set exists for {} pairs...", cfg.pair_count);
    let counter = Arc::new(AtomicUsize::new(0));
    let worker_count = max(1, cfg.concurrency / 2);
    let mut joins = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let c = client.clone();
        let cfg = cfg.clone();
        let counter = counter.clone();
        joins.push(tokio::spawn(async move {
            let mut out = Vec::new();
            loop {
                let idx = counter.fetch_add(1, Ordering::Relaxed);
                if idx >= cfg.pair_count {
                    break;
                }
                let src_ext = format!("{}-{idx:08}", cfg.src_prefix);
                let dst_ext = format!("{}-{idx:08}", cfg.dst_prefix);
                let src = c.create_node(&cfg.src_type, Some(&src_ext), &[]).await?;
                let dst = c.create_node(&cfg.dst_type, Some(&dst_ext), &[]).await?;
                out.push((idx, src.node_id, dst.node_id));
            }
            Ok::<Vec<(usize, u64, u64)>, jetgraph_client::ClientError>(out)
        }));
    }

    let mut node_ids: Vec<Option<(u64, u64)>> = vec![None; cfg.pair_count];
    for j in joins {
        let r = j.await?;
        match r {
            Ok(pairs) => {
                for (idx, src_id, dst_id) in pairs {
                    node_ids[idx] = Some((src_id, dst_id));
                }
            }
            Err(e) => {
                return Err(format!("failed to ensure nodes; check schema and node types: {e}").into());
            }
        }
    }
    println!("Node set ready.");

    let mut pairs = Vec::with_capacity(cfg.pair_count);
    for (idx, ids) in node_ids.into_iter().enumerate() {
        let (src_id, dst_id) = ids.ok_or_else(|| format!("internal error: missing node ids for pair index {idx}"))?;
        if cfg.use_node_ids {
            pairs.push((NodeRef::node_id(src_id), NodeRef::node_id(dst_id)));
        } else {
            let src_ext = format!("{}-{idx:08}", cfg.src_prefix);
            let dst_ext = format!("{}-{idx:08}", cfg.dst_prefix);
            pairs.push((
                NodeRef::external(&cfg.src_type, &src_ext),
                NodeRef::external(&cfg.dst_type, &dst_ext),
            ));
        }
    }

    println!(
        "Request ref mode: {}",
        if cfg.use_node_ids { "node_id (fast path)" } else { "external refs" }
    );
    Ok(WorkloadData { pairs: Arc::new(pairs) })
}

async fn ensure_schema(client: &Client, cfg: &Config) -> Result<(), Box<dyn Error>> {
    let mut schema = client.schema();
    let state = schema.get_schema().await?;

    let has_src = state.node_types.iter().any(|n| n.name == cfg.src_type);
    let has_dst = state.node_types.iter().any(|n| n.name == cfg.dst_type);
    let has_edge = state.edge_types.iter().any(|e| e.name == cfg.edge_type);

    if has_src && has_dst && has_edge {
        return Ok(());
    }

    if !cfg.bootstrap_schema {
        let node_names: Vec<String> = state.node_types.into_iter().map(|n| n.name).collect();
        let edge_names: Vec<String> = state.edge_types.into_iter().map(|e| e.name).collect();
        return Err(format!(
            "schema preflight failed: src_type='{}' present={} dst_type='{}' present={} edge_type='{}' present={}. \
Use existing schema names or rerun with --bootstrap-schema on a non-finalized schema.\n\
known_node_types={:?}\nknown_edge_types={:?}",
            cfg.src_type, has_src, cfg.dst_type, has_dst, cfg.edge_type, has_edge, node_names, edge_names
        ).into());
    }

    println!("Schema bootstrap requested. Registering missing types...");
    let mut s = client.schema();
    if !has_src {
        s.register_node_type(&cfg.src_type).await?;
    }
    if !has_dst {
        s.register_node_type(&cfg.dst_type).await?;
    }
    if !has_edge {
        s.register_compact_edge_type(
            &cfg.edge_type,
            &cfg.src_type,
            &cfg.dst_type,
            90 * 86_400,
            vec![10.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_000.0],
            "amount",
            3_600,
            None,
            false,
        )
        .await?;
    }
    let _ = s.finalize().await?;
    println!("Schema bootstrap complete.");
    Ok(())
}

async fn seed_edges_for_queries(client: &Client, cfg: &Config, data: &WorkloadData) -> Result<(), Box<dyn Error>> {
    let seed_reqs = cfg.pair_count.min(20_000);
    if seed_reqs == 0 {
        return Ok(());
    }
    println!("Seeding {seed_reqs} edges for query hit-rate...");
    let mut seeded = 0usize;
    let mut req_idx = 0usize;
    let start = Instant::now();
    while seeded < seed_reqs {
        let (src, dst) = pair_for(req_idx, cfg, &data.pairs);
        let ts = now_secs().saturating_sub((req_idx as u32) % cfg.event_span_secs.max(1));
        client
            .upsert_edge(&cfg.edge_type, src, dst, Some(((req_idx % 100) as f32) + 1.0), Some(ts), None)
            .await
            .map_err(|e| format!("failed to seed edges; check edge type and schema: {e}"))?;
        seeded += 1;
        req_idx += 1;
    }
    println!("Seed complete in {:.2}s", start.elapsed().as_secs_f64());
    Ok(())
}

async fn warmup_upsert(client: &Client, cfg: &Config, data: &WorkloadData) -> Result<(), Box<dyn Error>> {
    if cfg.warmup_requests == 0 {
        return Ok(());
    }
    println!("Warmup upsert: {} requests...", cfg.warmup_requests);
    let counter = Arc::new(AtomicUsize::new(0));
    let mut joins = Vec::with_capacity(cfg.concurrency);
    for worker_id in 0..cfg.concurrency {
        let c = client.clone();
        let cfg = cfg.clone();
        let counter = counter.clone();
        let pairs = data.pairs.clone();
        joins.push(tokio::spawn(async move {
            let mut rng = XorShift64::new(worker_id as u64 + 1_000_000);
            loop {
                let req_idx = counter.fetch_add(1, Ordering::Relaxed);
                if req_idx >= cfg.warmup_requests {
                    break;
                }
                let (src, dst) = pair_for(req_idx, &cfg, &pairs);
                let val = 1.0 + ((rng.next_u64() % 1000) as f32 / 10.0);
                let offset = (rng.next_u64() as u32) % cfg.event_span_secs.max(1);
                let ts = now_secs().saturating_sub(offset);
                c.upsert_edge(&cfg.edge_type, src, dst, Some(val), Some(ts), None).await?;
            }
            Ok::<(), jetgraph_client::ClientError>(())
        }));
    }
    for j in joins {
        j.await??;
    }
    println!("Warmup upsert complete.");
    Ok(())
}

async fn warmup_query(client: &Client, cfg: &Config, data: &WorkloadData) -> Result<(), Box<dyn Error>> {
    if cfg.warmup_requests == 0 {
        return Ok(());
    }
    println!("Warmup query: {} requests...", cfg.warmup_requests);
    let counter = Arc::new(AtomicUsize::new(0));
    let mut joins = Vec::with_capacity(cfg.concurrency);
    for _ in 0..cfg.concurrency {
        let c = client.clone();
        let cfg = cfg.clone();
        let counter = counter.clone();
        let pairs = data.pairs.clone();
        joins.push(tokio::spawn(async move {
            loop {
                let req_idx = counter.fetch_add(1, Ordering::Relaxed);
                if req_idx >= cfg.warmup_requests {
                    break;
                }
                let (src, dst) = pair_for(req_idx, &cfg, &pairs);
                c.get_edge_state(&cfg.edge_type, src, dst, None, None).await?;
            }
            Ok::<(), jetgraph_client::ClientError>(())
        }));
    }
    for j in joins {
        j.await??;
    }
    println!("Warmup query complete.");
    Ok(())
}

async fn run_upsert_load(clients: Vec<Client>, cfg: Config, data: WorkloadData) -> Result<RunSummary, Box<dyn Error>> {
    println!(
        "Running upsert load: requests={} concurrency={}...",
        cfg.upsert_requests, cfg.concurrency
    );
    let counter = Arc::new(AtomicUsize::new(0));
    let mut joins = Vec::with_capacity(cfg.concurrency);
    let start = Instant::now();

    for (worker_id, c) in clients.into_iter().enumerate() {
        let _ = worker_id;
        let cfg = cfg.clone();
        let counter = counter.clone();
        let pairs = data.pairs.clone();
        joins.push(tokio::spawn(async move {
            let mut rng = XorShift64::new(worker_id as u64 + 42);
            let mut stats = WorkerStats::default();
            loop {
                let req_idx = counter.fetch_add(1, Ordering::Relaxed);
                if req_idx >= cfg.upsert_requests {
                    break;
                }
                let (src, dst) = pair_for(req_idx, &cfg, &pairs);
                let val = 1.0 + ((rng.next_u64() % 1000) as f32 / 10.0);
                let offset = (rng.next_u64() as u32) % cfg.event_span_secs.max(1);
                let ts = now_secs().saturating_sub(offset);
                let t0 = Instant::now();
                match c.upsert_edge(&cfg.edge_type, src, dst, Some(val), Some(ts), None).await {
                    Ok(_) => {
                        stats.ok_count += 1;
                    }
                    Err(_) => {
                        stats.err_count += 1;
                    }
                }
                stats
                    .latencies_micros
                    .push(t0.elapsed().as_micros() as u64);
            }
            stats
        }));
    }

    let mut merged = WorkerStats::default();
    for j in joins {
        let s = j.await?;
        merged.ok_count += s.ok_count;
        merged.err_count += s.err_count;
        merged.miss_count += s.miss_count;
        merged.latencies_micros.extend(s.latencies_micros);
    }

    Ok(RunSummary {
        name: "upsert",
        elapsed: start.elapsed(),
        ok_count: merged.ok_count,
        err_count: merged.err_count,
        miss_count: merged.miss_count,
        latencies_micros: merged.latencies_micros,
    })
}

async fn run_query_load(clients: Vec<Client>, cfg: Config, data: WorkloadData) -> Result<RunSummary, Box<dyn Error>> {
    println!(
        "Running query load: requests={} concurrency={}...",
        cfg.query_requests, cfg.concurrency
    );
    let counter = Arc::new(AtomicUsize::new(0));
    let mut joins = Vec::with_capacity(cfg.concurrency);
    let start = Instant::now();

    for c in clients.into_iter() {
        let cfg = cfg.clone();
        let counter = counter.clone();
        let pairs = data.pairs.clone();
        joins.push(tokio::spawn(async move {
            let mut stats = WorkerStats::default();
            loop {
                let req_idx = counter.fetch_add(1, Ordering::Relaxed);
                if req_idx >= cfg.query_requests {
                    break;
                }
                let (src, dst) = pair_for(req_idx, &cfg, &pairs);
                let t0 = Instant::now();
                match c.get_edge_state(&cfg.edge_type, src, dst, None, None).await {
                    Ok(Some(_)) => {
                        stats.ok_count += 1;
                    }
                    Ok(None) => {
                        stats.ok_count += 1;
                        stats.miss_count += 1;
                    }
                    Err(_) => {
                        stats.err_count += 1;
                    }
                }
                stats
                    .latencies_micros
                    .push(t0.elapsed().as_micros() as u64);
            }
            stats
        }));
    }

    let mut merged = WorkerStats::default();
    for j in joins {
        let s = j.await?;
        merged.ok_count += s.ok_count;
        merged.err_count += s.err_count;
        merged.miss_count += s.miss_count;
        merged.latencies_micros.extend(s.latencies_micros);
    }

    Ok(RunSummary {
        name: "query",
        elapsed: start.elapsed(),
        ok_count: merged.ok_count,
        err_count: merged.err_count,
        miss_count: merged.miss_count,
        latencies_micros: merged.latencies_micros,
    })
}

fn print_summary(summary: &RunSummary) {
    let total = summary.ok_count + summary.err_count;
    let elapsed_s = summary.elapsed.as_secs_f64().max(0.000_001);
    let throughput = total as f64 / elapsed_s;

    let mut latencies = summary.latencies_micros.clone();
    latencies.sort_unstable();

    let avg = average_micros(&latencies);

    println!("\n=== {} summary ===", summary.name);
    println!("total_requests   : {total}");
    println!("ok_requests      : {}", summary.ok_count);
    println!("error_requests   : {}", summary.err_count);
    if summary.name == "query" {
        println!("query_misses     : {}", summary.miss_count);
    }
    println!("elapsed_seconds  : {:.3}", elapsed_s);
    println!("throughput_ops_s : {:.2}", throughput);
    println!("lat_avg_ms       : {:.3}", avg / 1000.0);
    println!("lat_p50_ms       : {:.3}", percentile(&latencies, 50.0) / 1000.0);
    println!("lat_p95_ms       : {:.3}", percentile(&latencies, 95.0) / 1000.0);
    println!("lat_p99_ms       : {:.3}", percentile(&latencies, 99.0) / 1000.0);
    println!("lat_max_ms       : {:.3}", latencies.last().copied().unwrap_or(0) as f64 / 1000.0);
}

fn enforce_sub_ms_if_requested(cfg: &Config, summary: &RunSummary) -> Result<(), Box<dyn Error>> {
    if !cfg.require_sub_ms {
        return Ok(());
    }
    let avg_ms = average_micros(&summary.latencies_micros) / 1000.0;
    if avg_ms >= 1.0 {
        return Err(format!(
            "SLO failed for {}: lat_avg_ms={:.3} (target: < 1.000 ms)",
            summary.name, avg_ms
        )
        .into());
    }
    println!(
        "SLO passed for {}: lat_avg_ms={:.3} (< 1.000 ms)",
        summary.name, avg_ms
    );
    Ok(())
}

fn average_micros(latencies: &[u64]) -> f64 {
    if latencies.is_empty() {
        0.0
    } else {
        latencies.iter().map(|v| *v as f64).sum::<f64>() / latencies.len() as f64
    }
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let n = sorted.len();
    let idx = (((p / 100.0) * ((n - 1) as f64)).round() as usize).min(n - 1);
    sorted[idx] as f64
}

struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x9e37_79b9_7f4a_7c15,
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
}
