use bolt4rs::{ConfigBuilder, Graph};
use clap::{Parser, ValueEnum};
use std::{
    error::Error,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::Barrier;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Workload {
    Read,
    Write,
    Mixed,
}

impl Workload {
    fn as_str(self) -> &'static str {
        match self {
            Workload::Read => "read",
            Workload::Write => "write",
            Workload::Mixed => "mixed",
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "bench_client")]
#[command(about = "Run Bolt4rs server throughput/latency benchmarks")]
struct Args {
    /// The URI of the Bolt server.
    #[arg(long, default_value = "127.0.0.1:7687")]
    uri: String,

    /// Number of concurrent worker tasks.
    #[arg(long)]
    threads: usize,

    /// Workload mix to run.
    #[arg(long, value_enum)]
    workload: Workload,

    /// Measurement duration in seconds.
    #[arg(long, default_value_t = 10)]
    duration_secs: u64,

    /// Number of rows to seed for read workloads.
    #[arg(long, default_value_t = 1024)]
    seed_rows: u64,
}

#[derive(Debug)]
struct WorkerResult {
    ops: u64,
    latencies_us: Vec<u128>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();
    if args.threads == 0 {
        return Err("threads must be greater than zero".into());
    }
    if args.seed_rows == 0 && matches!(args.workload, Workload::Read | Workload::Mixed) {
        return Err("seed rows must be greater than zero for read workloads".into());
    }

    let graph = Graph::connect(
        ConfigBuilder::default()
            .uri(&args.uri)
            .user("bolt")
            .password("test")
            .max_connections(args.threads + 4)
            .fetch_size(1024)
            .build()?,
    )?;

    setup(&graph, args.workload, args.seed_rows).await?;

    let barrier = Arc::new(Barrier::new(args.threads + 1));
    let write_id = Arc::new(AtomicU64::new(args.seed_rows + 1));
    let deadline = Instant::now() + Duration::from_secs(args.duration_secs);
    let mut handles = Vec::with_capacity(args.threads);

    for worker_id in 0..args.threads {
        let worker_graph = graph.clone();
        let worker_barrier = barrier.clone();
        let worker_write_id = write_id.clone();
        let workload = args.workload;
        let seed_rows = args.seed_rows;

        handles.push(tokio::spawn(async move {
            worker_barrier.wait().await;
            run_worker(
                worker_id as u64,
                worker_graph,
                workload,
                seed_rows,
                worker_write_id,
                deadline,
            )
            .await
        }));
    }

    barrier.wait().await;
    let started = Instant::now();

    let mut ops = 0_u64;
    let mut latencies_us = Vec::new();
    for handle in handles {
        let result = handle.await??;
        ops += result.ops;
        latencies_us.extend(result.latencies_us);
    }

    let elapsed = started.elapsed().as_secs_f64();
    latencies_us.sort_unstable();
    let p99_us = percentile(&latencies_us, 99.0).unwrap_or(0);
    let throughput = ops as f64 / elapsed;

    println!("workload,threads,ops,elapsed_secs,throughput_ops_sec,p99_latency_us");
    println!(
        "{},{},{},{:.3},{:.2},{}",
        args.workload.as_str(),
        args.threads,
        ops,
        elapsed,
        throughput,
        p99_us
    );

    Ok(())
}

async fn setup(
    graph: &Graph,
    workload: Workload,
    seed_rows: u64,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    graph
        .run("CREATE NODE TABLE Bench(id INT64, value INT64, PRIMARY KEY(id));")
        .await?;

    if matches!(workload, Workload::Read | Workload::Mixed) {
        for id in 0..seed_rows {
            graph
                .run(format!(
                    "CREATE (:Bench {{id: {}, value: {}}});",
                    id,
                    id * 2
                ))
                .await?;
        }
    }

    Ok(())
}

async fn run_worker(
    worker_id: u64,
    graph: Graph,
    workload: Workload,
    seed_rows: u64,
    write_id: Arc<AtomicU64>,
    deadline: Instant,
) -> Result<WorkerResult, Box<dyn Error + Send + Sync>> {
    let mut ops = 0_u64;
    let mut latencies_us = Vec::new();

    while Instant::now() < deadline {
        let op_start = Instant::now();
        match workload {
            Workload::Read => read_one(&graph, (worker_id + ops) % seed_rows).await?,
            Workload::Write => write_one(&graph, write_id.fetch_add(1, Ordering::Relaxed)).await?,
            Workload::Mixed => {
                if ops % 2 == 0 {
                    read_one(&graph, (worker_id + ops) % seed_rows).await?;
                } else {
                    write_one(&graph, write_id.fetch_add(1, Ordering::Relaxed)).await?;
                }
            }
        }
        latencies_us.push(op_start.elapsed().as_micros());
        ops += 1;
    }

    Ok(WorkerResult { ops, latencies_us })
}

async fn read_one(graph: &Graph, id: u64) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut stream = graph
        .execute(format!(
            "MATCH (b:Bench) WHERE b.id = {} RETURN b.value AS value;",
            id
        ))
        .await?;

    while stream.next().await?.is_some() {}
    stream.finish().await?;
    Ok(())
}

async fn write_one(graph: &Graph, id: u64) -> Result<(), Box<dyn Error + Send + Sync>> {
    graph
        .run(format!(
            "CREATE (:Bench {{id: {}, value: {}}});",
            id,
            id * 2
        ))
        .await?;
    Ok(())
}

fn percentile(values: &[u128], percentile: f64) -> Option<u128> {
    if values.is_empty() {
        return None;
    }
    let rank = ((percentile / 100.0) * values.len() as f64).ceil() as usize;
    let index = rank.saturating_sub(1).min(values.len() - 1);
    Some(values[index])
}
