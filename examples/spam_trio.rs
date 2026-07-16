use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

#[derive(Clone, Copy, Debug)]
enum Role {
    ApiGateway,
    JobWorker,
    DbServer,
}

impl Role {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "api" | "api-gateway" | "gateway" => Some(Self::ApiGateway),
            "worker" | "job-worker" | "jobs" => Some(Self::JobWorker),
            "db" | "db-server" | "database" => Some(Self::DbServer),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::ApiGateway => "api-gateway",
            Self::JobWorker => "job-worker",
            Self::DbServer => "db-server",
        }
    }
}

#[derive(Debug)]
struct Args {
    role: Role,
    log_file: Option<PathBuf>,
    duration: Option<Duration>,
    burst: usize,
    tick: Duration,
    payload_bytes: usize,
    flush_every: usize,
    service_id: String,
}

impl Args {
    fn parse() -> Self {
        let mut args = env::args().skip(1);
        let mut parsed = Self {
            role: Role::ApiGateway,
            log_file: None,
            duration: None,
            burst: 100,
            tick: Duration::from_millis(100),
            payload_bytes: 24,
            flush_every: 100,
            service_id: "local".to_string(),
        };

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--role" => {
                    let value = args.next().unwrap_or_else(|| {
                        usage_exit("--role requires api-gateway, job-worker, or db-server")
                    });
                    parsed.role = Role::parse(&value).unwrap_or_else(|| {
                        usage_exit("unknown --role; use api-gateway, job-worker, or db-server")
                    });
                }
                "--log-file" => {
                    parsed.log_file = Some(PathBuf::from(
                        args.next()
                            .unwrap_or_else(|| usage_exit("--log-file requires a path")),
                    ));
                }
                "--stdout" => parsed.log_file = None,
                "--duration-seconds" | "--duration" => {
                    let seconds = args
                        .next()
                        .and_then(|value| value.parse::<u64>().ok())
                        .unwrap_or_else(|| usage_exit("--duration-seconds requires a number"));
                    parsed.duration = Some(Duration::from_secs(seconds));
                }
                "--burst" => {
                    parsed.burst = args
                        .next()
                        .and_then(|value| value.parse().ok())
                        .filter(|value| *value > 0)
                        .unwrap_or_else(|| usage_exit("--burst requires a positive number"));
                }
                "--tick-ms" => {
                    let millis = args
                        .next()
                        .and_then(|value| value.parse::<u64>().ok())
                        .filter(|value| *value > 0)
                        .unwrap_or_else(|| usage_exit("--tick-ms requires a positive number"));
                    parsed.tick = Duration::from_millis(millis);
                }
                "--payload-bytes" => {
                    parsed.payload_bytes = args
                        .next()
                        .and_then(|value| value.parse().ok())
                        .unwrap_or_else(|| usage_exit("--payload-bytes requires a number"));
                }
                "--flush-every" => {
                    parsed.flush_every = args
                        .next()
                        .and_then(|value| value.parse().ok())
                        .filter(|value| *value > 0)
                        .unwrap_or_else(|| usage_exit("--flush-every requires a positive number"));
                }
                "--service-id" => {
                    parsed.service_id = args
                        .next()
                        .unwrap_or_else(|| usage_exit("--service-id requires a value"));
                }
                "--help" | "-h" => usage_exit(""),
                other => usage_exit(&format!("unknown argument: {other}")),
            }
        }

        parsed
    }
}

fn usage_exit(message: &str) -> ! {
    if !message.is_empty() {
        eprintln!("spam_trio: {message}");
    }

    eprintln!(
        "usage: spam_trio --role api-gateway|job-worker|db-server [--log-file PATH|--stdout] [--duration-seconds N] [--burst N] [--tick-ms N] [--payload-bytes N]"
    );
    std::process::exit(if message.is_empty() { 0 } else { 2 });
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    if let Some(path) = args.log_file.as_ref().and_then(|path| path.parent()) {
        fs::create_dir_all(path)?;
    }

    let writer: Box<dyn Write> = match &args.log_file {
        Some(path) => Box::new(OpenOptions::new().create(true).append(true).open(path)?),
        None => Box::new(io::stdout()),
    };
    let mut writer = BufWriter::new(writer);
    let mut rng = Lcg::new(seed_for(args.role, &args.service_id));
    let started = Instant::now();
    let mut sequence = 0_u64;

    loop {
        if let Some(duration) = args.duration {
            if started.elapsed() >= duration {
                break;
            }
        }

        for _ in 0..args.burst {
            sequence += 1;
            let line = spam_line(&args, sequence, &mut rng);
            writeln!(writer, "{line}")?;

            if sequence as usize % args.flush_every == 0 {
                writer.flush()?;
            }
        }

        writer.flush()?;
        thread::sleep(args.tick);
    }

    writer.flush()
}

fn spam_line(args: &Args, sequence: u64, rng: &mut Lcg) -> String {
    match args.role {
        Role::ApiGateway => api_gateway_line(args, sequence, rng),
        Role::JobWorker => job_worker_line(args, sequence, rng),
        Role::DbServer => db_server_line(args, sequence, rng),
    }
}

fn api_gateway_line(args: &Args, sequence: u64, rng: &mut Lcg) -> String {
    let route = choose(
        rng,
        &["/graphql", "/graphql/batch", "/healthz", "/admin/cache"],
    );
    let operation = choose(
        rng,
        &[
            "CheckoutSummary",
            "SearchCatalog",
            "UpdateCart",
            "IntrospectionQuery",
            "UserRecommendations",
        ],
    );
    let latency = rng.range(3, 900);
    let status = if sequence % 997 == 0 { 500 } else { 200 };
    let level = if status >= 500 {
        "ERROR"
    } else if latency > 650 {
        "WARN"
    } else {
        choose(rng, &["INFO", "DEBUG"])
    };
    let event = if route == "/healthz" {
        "healthcheck"
    } else if operation == "IntrospectionQuery" {
        "graphql.introspection"
    } else {
        "graphql.request"
    };

    format!(
        "ts={} service={} instance={} level={} event={} route={} graphql_op={} request_id=req-{:016x} trace_id=trc-{:016x} status={} latency_ms={} slow={} payload={} msg={}",
        unix_millis(),
        args.role.as_str(),
        args.service_id,
        level,
        event,
        route,
        operation,
        sequence,
        rng.next(),
        status,
        latency,
        latency > 650,
        payload(rng, args.payload_bytes),
        if status >= 500 {
            "gateway_upstream_failure"
        } else {
            "gateway_dispatched_graphql"
        }
    )
}

fn job_worker_line(args: &Args, sequence: u64, rng: &mut Lcg) -> String {
    let queue = choose(
        rng,
        &["email", "payments", "indexing", "graphql-cache", "exports"],
    );
    let event = choose(
        rng,
        &[
            "job.dequeue",
            "job.heartbeat",
            "job.retry",
            "job.complete",
            "job.deadletter",
        ],
    );
    let attempts = rng.range(1, 5);
    let latency = rng.range(10, 5_000);
    let level = if event == "job.deadletter" {
        "ERROR"
    } else if event == "job.retry" || latency > 3_500 {
        "WARN"
    } else {
        "INFO"
    };

    format!(
        "ts={} service={} instance={} level={} event={} queue={} job_id=job-{:016x} attempt={} latency_ms={} slow={} payload={} msg={}",
        unix_millis(),
        args.role.as_str(),
        args.service_id,
        level,
        event,
        queue,
        sequence,
        attempts,
        latency,
        latency > 3_500,
        payload(rng, args.payload_bytes),
        if event == "job.deadletter" {
            "worker_exhausted_retries"
        } else {
            "worker_processed_job"
        }
    )
}

fn db_server_line(args: &Args, sequence: u64, rng: &mut Lcg) -> String {
    let query = choose(
        rng,
        &[
            "SELECT_cart_by_user",
            "SELECT_catalog_search",
            "UPDATE_inventory_hold",
            "INSERT_job_result",
            "VACUUM_heartbeat",
            "LOCK_wait_probe",
        ],
    );
    let latency = rng.range(1, 2_500);
    let rows = rng.range(0, 10_000);
    let event = if query == "VACUUM_heartbeat" {
        "db.vacuum_heartbeat"
    } else if query == "LOCK_wait_probe" {
        "db.lock_wait"
    } else {
        "db.query"
    };
    let level = if sequence % 733 == 0 {
        "ERROR"
    } else if latency > 1_600 || event == "db.lock_wait" {
        "WARN"
    } else {
        choose(rng, &["INFO", "DEBUG"])
    };

    format!(
        "ts={} service={} instance={} level={} event={} bolt_on=stdout_pipe query={} txn=txn-{:016x} rows={} latency_ms={} slow={} payload={} msg={}",
        unix_millis(),
        args.role.as_str(),
        args.service_id,
        level,
        event,
        query,
        sequence,
        rows,
        latency,
        latency > 1_600,
        payload(rng, args.payload_bytes),
        if level == "ERROR" {
            "db_connection_reset"
        } else {
            "db_served_legacy_stdout"
        }
    )
}

fn choose<'a>(rng: &mut Lcg, values: &'a [&'a str]) -> &'a str {
    values[rng.range(0, values.len() as u64 - 1) as usize]
}

fn payload(rng: &mut Lcg, bytes: usize) -> String {
    let mut out = String::with_capacity(bytes);

    while out.len() < bytes {
        out.push_str(&format!("{:016x}", rng.next()));
    }

    out.truncate(bytes);
    out
}

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

fn seed_for(role: Role, service_id: &str) -> u64 {
    let mut seed: u64 = match role {
        Role::ApiGateway => 0xaced_1001,
        Role::JobWorker => 0xaced_2002,
        Role::DbServer => 0xaced_3003,
    };

    for byte in service_id.bytes() {
        seed = seed.rotate_left(5) ^ u64::from(byte);
    }

    seed
}

struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }

    fn range(&mut self, min: u64, max: u64) -> u64 {
        min + (self.next() % (max - min + 1))
    }
}
