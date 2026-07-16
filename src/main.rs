use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::env;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::net::{Shutdown, TcpListener, TcpStream, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

const DEFAULT_CONFIG: &str = "/etc/edgelog/filter.conf";
const POLL_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_RING_LINE_BYTES: usize = 512;
const DEFAULT_RING_MAX_BYTES: usize = 64 * 1024;
const MIN_RING_MAX_BYTES: usize = 1024;
const AUDIT_TOTAL_BUDGET_BYTES: usize = 32 * 1024;
const AUDIT_MASTER_BUDGET_BYTES: usize = 1024;
const AUDIT_PAYLOAD_BUDGET_BYTES: usize = AUDIT_TOTAL_BUDGET_BYTES - AUDIT_MASTER_BUDGET_BYTES;
const AUDIT_MAX_SESSIONS: usize = 64;
const AUDIT_PREVIEW_BYTES: usize = 64;
const TUNNEL_COPY_BUFFER_BYTES: usize = 8192;

#[derive(Clone, Debug)]
struct Filter {
    mode: Mode,
    patterns: Vec<String>,
    rings: Vec<RingConfig>,
    hops: Vec<HopConfig>,
    peers: Vec<PeerConfig>,
    upstreams: Vec<UpstreamConfig>,
    tunnels: Vec<TunnelConfig>,
    sample_every: usize,
    throttle_per_second: Option<u64>,
    output: OutputConfig,
    metrics: MetricsConfig,
    traces: TraceConfig,
    temporary_rules: Vec<TemporaryRule>,
}

#[derive(Clone, Debug)]
struct RingConfig {
    name: String,
    capacity: usize,
    pattern: String,
    byte_budget: usize,
}

#[derive(Clone, Debug)]
struct HopConfig {
    name: String,
    pattern: String,
}

#[derive(Clone, Debug)]
struct PeerConfig {
    name: String,
    addr: String,
}

#[derive(Clone, Debug)]
struct UpstreamConfig {
    addr: String,
}

#[derive(Clone, Debug)]
struct TunnelConfig {
    name: String,
    addr: String,
    kind: TunnelKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TunnelKind {
    Tcp,
    Http,
    WebSocket,
    Debugger,
}

impl TunnelKind {
    fn as_str(self) -> &'static str {
        match self {
            TunnelKind::Tcp => "tcp",
            TunnelKind::Http => "http",
            TunnelKind::WebSocket => "websocket",
            TunnelKind::Debugger => "debugger",
        }
    }
}

#[derive(Clone, Debug)]
struct MetricsConfig {
    enabled: bool,
    statsd_addr: Option<String>,
    statsd_prefix: String,
    labels: MetricLabels,
}

#[derive(Clone, Debug)]
struct MetricLabels {
    node: bool,
    ring: bool,
    hop: bool,
    command: bool,
    outcome: bool,
}

#[derive(Clone, Debug)]
struct TraceConfig {
    enabled: bool,
    file_path: Option<PathBuf>,
    tcp_addr: Option<String>,
    sample_every: usize,
    include_line: bool,
}

#[derive(Clone, Debug)]
struct OutputConfig {
    stdout_enabled: bool,
    stdout_tag: bool,
    stdout_prefix: Option<String>,
}

#[derive(Clone, Copy, Debug)]
enum MetricLabel {
    Node,
    Ring,
    Hop,
    Command,
    Outcome,
}

#[derive(Clone, Debug)]
struct TemporaryRule {
    name: String,
    duration: Duration,
    directive_text: String,
    rule: Rule,
}

#[derive(Clone, Debug)]
struct TemporaryGroup {
    name: String,
    duration: Duration,
    fingerprint: String,
    rules: Vec<Rule>,
}

#[derive(Clone, Debug)]
struct TemporaryState {
    name: String,
    fingerprint: String,
    expires_at: Instant,
    expired_reported: bool,
}

#[derive(Clone, Debug)]
enum Rule {
    Mode(Mode),
    Pattern(String),
    Ring(RingConfig),
    Hop(HopConfig),
    Peer(PeerConfig),
    Upstream(UpstreamConfig),
    Tunnel(TunnelConfig),
    SampleEvery(usize),
    ThrottlePerSecond(u64),
    StdoutEnabled(bool),
    StdoutTag(bool),
    StdoutPrefix(Option<String>),
    MetricsEnabled(bool),
    StatsdAddr(Option<String>),
    StatsdPrefix(String),
    MetricLabel(MetricLabel, bool),
    TracesEnabled(bool),
    TraceFile(Option<PathBuf>),
    TraceTcp(Option<String>),
    TraceSampleEvery(usize),
    TraceLine(bool),
    ClearPatterns,
    ClearThrottle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Include,
    Exclude,
}

impl Default for Filter {
    fn default() -> Self {
        Self {
            mode: Mode::Include,
            patterns: Vec::new(),
            rings: Vec::new(),
            hops: Vec::new(),
            peers: Vec::new(),
            upstreams: Vec::new(),
            tunnels: Vec::new(),
            sample_every: 1,
            throttle_per_second: None,
            output: OutputConfig::default(),
            metrics: MetricsConfig::default(),
            traces: TraceConfig::default(),
            temporary_rules: Vec::new(),
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            statsd_addr: None,
            statsd_prefix: "edgelog".to_string(),
            labels: MetricLabels::default(),
        }
    }
}

impl Default for TraceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            file_path: None,
            tcp_addr: None,
            sample_every: 1,
            include_line: false,
        }
    }
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            stdout_enabled: true,
            stdout_tag: false,
            stdout_prefix: None,
        }
    }
}

impl Default for MetricLabels {
    fn default() -> Self {
        Self {
            node: true,
            ring: false,
            hop: false,
            command: false,
            outcome: true,
        }
    }
}

impl Filter {
    fn from_file(path: &Path) -> io::Result<Self> {
        let text = fs::read_to_string(path)?;
        let mut filter = Filter::default();

        for raw_line in text.lines() {
            let line = raw_line.trim();

            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if let Some(rest) = line.strip_prefix("temp ") {
                match parse_temporary_rule(rest) {
                    Ok(rule) => filter.temporary_rules.push(rule),
                    Err(error) => eprintln!("edgelog: invalid temp config line: {error}: {line}"),
                }
                continue;
            }

            match parse_rule(line) {
                Ok(rule) => apply_rule(&mut filter, rule),
                Err(error) => eprintln!("edgelog: invalid config line: {error}: {line}"),
            }
        }

        Ok(filter)
    }

    fn allows(&self, line: &str) -> bool {
        if self.patterns.is_empty() {
            return true;
        }

        let matched = self.patterns.iter().any(|pattern| line.contains(pattern));

        match self.mode {
            Mode::Include => matched,
            Mode::Exclude => !matched,
        }
    }
}

fn parse_temporary_rule(rest: &str) -> Result<TemporaryRule, String> {
    let mut parts = rest
        .splitn(3, char::is_whitespace)
        .filter(|part| !part.is_empty());
    let name = parts
        .next()
        .ok_or_else(|| "missing temporary rule name".to_string())?
        .to_string();
    let duration = parse_duration(
        parts
            .next()
            .ok_or_else(|| "missing temporary rule duration".to_string())?,
    )?;
    let directive_text = parts
        .next()
        .ok_or_else(|| "missing temporary rule directive".to_string())?
        .trim()
        .to_string();

    if name.contains('/') || name == "." || name == ".." {
        return Err("temporary rule name must be a file-safe name".to_string());
    }

    let rule = parse_rule(&directive_text)?;

    Ok(TemporaryRule {
        name,
        duration,
        directive_text,
        rule,
    })
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let value = value.trim();
    let split_at = value
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(value.len());
    let amount: u64 = value[..split_at]
        .parse()
        .map_err(|_| "duration must start with a positive number".to_string())?;
    let unit = &value[split_at..];

    if amount == 0 {
        return Err("duration must be greater than zero".to_string());
    }

    let seconds = match unit {
        "" | "s" => Ok(amount),
        "m" => amount
            .checked_mul(60)
            .ok_or_else(|| "duration is too large".to_string()),
        "h" => amount
            .checked_mul(60 * 60)
            .ok_or_else(|| "duration is too large".to_string()),
        _ => Err("duration unit must be s, m, h, or omitted for seconds".to_string()),
    }?;

    Ok(Duration::from_secs(seconds))
}

fn parse_rule(line: &str) -> Result<Rule, String> {
    if line == "clear_patterns" {
        return Ok(Rule::ClearPatterns);
    }

    if line == "clear_throttle" {
        return Ok(Rule::ClearThrottle);
    }

    if let Some(enabled) = line.strip_prefix("metrics=") {
        return Ok(Rule::MetricsEnabled(parse_on_off(enabled)?));
    }

    if let Some(addr) = line.strip_prefix("statsd ") {
        let addr = addr.trim();

        return if addr == "off" {
            Ok(Rule::StatsdAddr(None))
        } else if addr.is_empty() {
            Err("statsd requires HOST:PORT or off".to_string())
        } else {
            Ok(Rule::StatsdAddr(Some(addr.to_string())))
        };
    }

    if let Some(prefix) = line.strip_prefix("statsd_prefix=") {
        let prefix = prefix.trim();

        if prefix.is_empty() {
            return Err("statsd_prefix cannot be empty".to_string());
        }

        return Ok(Rule::StatsdPrefix(prefix.to_string()));
    }

    if let Some(rest) = line.strip_prefix("metric_label ") {
        let mut parts = rest.splitn(2, '=');
        let label = parse_metric_label(
            parts
                .next()
                .ok_or_else(|| "metric_label requires LABEL=on|off".to_string())?
                .trim(),
        )?;
        let enabled = parse_on_off(
            parts
                .next()
                .ok_or_else(|| "metric_label requires LABEL=on|off".to_string())?,
        )?;

        return Ok(Rule::MetricLabel(label, enabled));
    }

    if let Some(enabled) = line.strip_prefix("traces=") {
        return Ok(Rule::TracesEnabled(parse_on_off(enabled)?));
    }

    if let Some(path) = line.strip_prefix("trace_file ") {
        let path = path.trim();

        return if path == "off" {
            Ok(Rule::TraceFile(None))
        } else if path.is_empty() {
            Err("trace_file requires a path or off".to_string())
        } else {
            Ok(Rule::TraceFile(Some(PathBuf::from(path))))
        };
    }

    if let Some(addr) = line.strip_prefix("trace_tcp ") {
        let addr = addr.trim();

        return if addr == "off" {
            Ok(Rule::TraceTcp(None))
        } else if addr.is_empty() {
            Err("trace_tcp requires HOST:PORT or off".to_string())
        } else {
            Ok(Rule::TraceTcp(Some(addr.to_string())))
        };
    }

    if let Some(sample) = line.strip_prefix("trace_sample=") {
        return match sample.trim().parse() {
            Ok(0) | Err(_) => Err("trace_sample must be greater than zero".to_string()),
            Ok(value) => Ok(Rule::TraceSampleEvery(value)),
        };
    }

    if let Some(include_line) = line.strip_prefix("trace_line=") {
        return Ok(Rule::TraceLine(parse_on_off(include_line)?));
    }

    if let Some(mode) = line.strip_prefix("mode=") {
        return match mode.trim() {
            "include" => Ok(Rule::Mode(Mode::Include)),
            "exclude" => Ok(Rule::Mode(Mode::Exclude)),
            other => Err(format!("unknown mode '{other}'")),
        };
    }

    if let Some(sample) = line.strip_prefix("sample=") {
        return match sample.trim().parse() {
            Ok(0) | Err(_) => Err("sample must be greater than zero".to_string()),
            Ok(value) => Ok(Rule::SampleEvery(value)),
        };
    }

    if let Some(throttle) = line.strip_prefix("throttle_per_second=") {
        return match throttle.trim().parse() {
            Ok(0) | Err(_) => Err("throttle_per_second must be greater than zero".to_string()),
            Ok(value) => Ok(Rule::ThrottlePerSecond(value)),
        };
    }

    if let Some(enabled) = line.strip_prefix("stdout=") {
        return Ok(Rule::StdoutEnabled(parse_on_off(enabled)?));
    }

    if let Some(enabled) = line.strip_prefix("stdout_tag=") {
        return Ok(Rule::StdoutTag(parse_on_off(enabled)?));
    }

    if let Some(prefix) = line.strip_prefix("stdout_prefix=") {
        let prefix = prefix.trim();
        return if prefix.is_empty() || prefix == "off" {
            Ok(Rule::StdoutPrefix(None))
        } else {
            Ok(Rule::StdoutPrefix(Some(prefix.to_string())))
        };
    }

    if let Some(rest) = line.strip_prefix("ring ") {
        return parse_ring(rest)
            .map(Rule::Ring)
            .ok_or_else(|| "ring requires NAME CAPACITY PATTERN".to_string());
    }

    if let Some(rest) = line.strip_prefix("hop ") {
        return parse_hop(rest)
            .map(Rule::Hop)
            .ok_or_else(|| "hop requires NAME PATTERN".to_string());
    }

    if let Some(rest) = line.strip_prefix("peer ") {
        return parse_peer(rest)
            .map(Rule::Peer)
            .ok_or_else(|| "peer requires NAME HOST:PORT".to_string());
    }

    if let Some(rest) = line.strip_prefix("upstream ") {
        return parse_upstream(rest)
            .map(Rule::Upstream)
            .ok_or_else(|| "upstream requires HOST:PORT".to_string());
    }

    if let Some(rest) = line.strip_prefix("tunnel ") {
        return parse_tunnel(rest).map(Rule::Tunnel).ok_or_else(|| {
            "tunnel requires NAME HOST:PORT [tcp|http|websocket|debugger]".to_string()
        });
    }

    if line.starts_with("temp ") {
        return Err("nested temporary rules are not supported".to_string());
    }

    Ok(Rule::Pattern(line.to_string()))
}

fn apply_rule(filter: &mut Filter, rule: Rule) {
    match rule {
        Rule::Mode(mode) => filter.mode = mode,
        Rule::Pattern(pattern) => filter.patterns.push(pattern),
        Rule::Ring(ring) => filter.rings.push(ring),
        Rule::Hop(hop) => filter.hops.push(hop),
        Rule::Peer(peer) => filter.peers.push(peer),
        Rule::Upstream(upstream) => filter.upstreams.push(upstream),
        Rule::Tunnel(tunnel) => filter.tunnels.push(tunnel),
        Rule::SampleEvery(sample_every) => filter.sample_every = sample_every,
        Rule::ThrottlePerSecond(throttle_per_second) => {
            filter.throttle_per_second = Some(throttle_per_second);
        }
        Rule::StdoutEnabled(enabled) => filter.output.stdout_enabled = enabled,
        Rule::StdoutTag(enabled) => filter.output.stdout_tag = enabled,
        Rule::StdoutPrefix(prefix) => filter.output.stdout_prefix = prefix,
        Rule::MetricsEnabled(enabled) => filter.metrics.enabled = enabled,
        Rule::StatsdAddr(addr) => filter.metrics.statsd_addr = addr,
        Rule::StatsdPrefix(prefix) => filter.metrics.statsd_prefix = prefix,
        Rule::MetricLabel(label, enabled) => match label {
            MetricLabel::Node => filter.metrics.labels.node = enabled,
            MetricLabel::Ring => filter.metrics.labels.ring = enabled,
            MetricLabel::Hop => filter.metrics.labels.hop = enabled,
            MetricLabel::Command => filter.metrics.labels.command = enabled,
            MetricLabel::Outcome => filter.metrics.labels.outcome = enabled,
        },
        Rule::TracesEnabled(enabled) => filter.traces.enabled = enabled,
        Rule::TraceFile(path) => filter.traces.file_path = path,
        Rule::TraceTcp(addr) => filter.traces.tcp_addr = addr,
        Rule::TraceSampleEvery(sample_every) => filter.traces.sample_every = sample_every,
        Rule::TraceLine(include_line) => filter.traces.include_line = include_line,
        Rule::ClearPatterns => filter.patterns.clear(),
        Rule::ClearThrottle => filter.throttle_per_second = None,
    }
}

fn parse_on_off(value: &str) -> Result<bool, String> {
    match value.trim() {
        "on" | "true" | "yes" | "1" => Ok(true),
        "off" | "false" | "no" | "0" => Ok(false),
        _ => Err("expected on or off".to_string()),
    }
}

fn parse_metric_label(value: &str) -> Result<MetricLabel, String> {
    match value {
        "node" => Ok(MetricLabel::Node),
        "ring" => Ok(MetricLabel::Ring),
        "hop" => Ok(MetricLabel::Hop),
        "command" => Ok(MetricLabel::Command),
        "outcome" => Ok(MetricLabel::Outcome),
        _ => Err("metric label must be node, ring, hop, command, or outcome".to_string()),
    }
}

fn parse_hop(rest: &str) -> Option<HopConfig> {
    let mut parts = rest
        .splitn(2, char::is_whitespace)
        .filter(|part| !part.is_empty());
    let name = parts.next()?.to_string();
    let pattern = parts.next()?.trim().to_string();

    if name.contains('/') || name == "." || name == ".." || pattern.is_empty() {
        return None;
    }

    Some(HopConfig { name, pattern })
}

fn parse_peer(rest: &str) -> Option<PeerConfig> {
    let mut parts = rest
        .splitn(2, char::is_whitespace)
        .filter(|part| !part.is_empty());
    let name = parts.next()?.to_string();
    let addr = parts.next()?.trim().to_string();

    if name.contains('/') || name == "." || name == ".." || addr.is_empty() {
        return None;
    }

    Some(PeerConfig { name, addr })
}

fn parse_upstream(rest: &str) -> Option<UpstreamConfig> {
    let addr = rest.trim().to_string();

    if addr.is_empty() {
        return None;
    }

    Some(UpstreamConfig { addr })
}

fn parse_tunnel(rest: &str) -> Option<TunnelConfig> {
    let mut parts = rest.split_whitespace();
    let name = parts.next()?.to_string();
    let addr = parts.next()?.to_string();
    let kind = match parts.next().unwrap_or("tcp") {
        "tcp" => TunnelKind::Tcp,
        "http" => TunnelKind::Http,
        "websocket" | "ws" => TunnelKind::WebSocket,
        "debugger" | "debug" => TunnelKind::Debugger,
        _ => return None,
    };

    if !is_safe_control_name(&name)
        || addr.is_empty()
        || !addr.contains(':')
        || parts.next().is_some()
    {
        return None;
    }

    Some(TunnelConfig { name, addr, kind })
}

fn parse_ring(rest: &str) -> Option<RingConfig> {
    let mut parts = rest
        .splitn(3, char::is_whitespace)
        .filter(|part| !part.is_empty());
    let name = parts.next()?.to_string();
    let capacity = parts.next()?.parse().ok()?;
    let mut pattern = parts.next()?.trim().to_string();
    let mut byte_budget = default_ring_byte_budget(capacity);

    if let Some((left, right)) = pattern.rsplit_once(" max_bytes=") {
        let parsed_budget = right.trim().parse().ok()?;
        if parsed_budget < MIN_RING_MAX_BYTES {
            return None;
        }
        pattern = left.trim_end().to_string();
        byte_budget = parsed_budget;
    }

    if name.contains('/') || name == "." || name == ".." || capacity == 0 || pattern.is_empty() {
        return None;
    }

    Some(RingConfig {
        name,
        capacity,
        pattern,
        byte_budget,
    })
}

fn default_ring_byte_budget(capacity: usize) -> usize {
    capacity
        .saturating_mul(DEFAULT_RING_LINE_BYTES)
        .clamp(MIN_RING_MAX_BYTES, DEFAULT_RING_MAX_BYTES)
}

fn is_safe_control_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
}

struct LiveFilter {
    path: PathBuf,
    modified_at: Option<SystemTime>,
    current: Filter,
    temporary_states: Vec<TemporaryState>,
}

impl LiveFilter {
    fn new(path: PathBuf) -> Self {
        let mut live = Self {
            path,
            modified_at: None,
            current: Filter::default(),
            temporary_states: Vec::new(),
        };
        live.reload_if_changed();
        live
    }

    fn snapshot(&mut self) -> Filter {
        self.reload_if_changed();
        self.report_expired_temporary_rules();

        let mut effective = self.current.clone();
        effective.temporary_rules.clear();

        let now = Instant::now();
        for group in temporary_groups(&self.current.temporary_rules) {
            let Some(state) = self
                .temporary_states
                .iter()
                .find(|state| state.name == group.name && state.fingerprint == group.fingerprint)
            else {
                continue;
            };

            if now >= state.expires_at {
                continue;
            }

            for rule in group.rules {
                apply_rule(&mut effective, rule);
            }
        }

        effective
    }

    fn reload_if_changed(&mut self) {
        let modified_at = fs::metadata(&self.path)
            .and_then(|metadata| metadata.modified())
            .ok();

        if modified_at == self.modified_at {
            return;
        }

        match Filter::from_file(&self.path) {
            Ok(filter) => {
                eprintln!(
                    "edgelog: loaded {} pattern(s) from {} in {:?} mode",
                    filter.patterns.len(),
                    self.path.display(),
                    filter.mode
                );
                self.current = filter;
                self.modified_at = modified_at;
                self.reconcile_temporary_rules();
            }
            Err(error) => {
                eprintln!(
                    "edgelog: could not load {}: {error}; keeping previous filter",
                    self.path.display()
                );
                self.modified_at = modified_at;
            }
        }
    }

    fn reconcile_temporary_rules(&mut self) {
        let groups = temporary_groups(&self.current.temporary_rules);
        let now = Instant::now();

        self.temporary_states.retain(|state| {
            groups
                .iter()
                .any(|group| group.name == state.name && group.fingerprint == state.fingerprint)
        });

        for group in groups {
            if self
                .temporary_states
                .iter()
                .any(|state| state.name == group.name && state.fingerprint == group.fingerprint)
            {
                continue;
            }

            eprintln!(
                "edgelog: temporary rule '{}' active for {}s",
                group.name,
                group.duration.as_secs()
            );
            self.temporary_states.push(TemporaryState {
                name: group.name,
                fingerprint: group.fingerprint,
                expires_at: now + group.duration,
                expired_reported: false,
            });
        }
    }

    fn report_expired_temporary_rules(&mut self) {
        let now = Instant::now();

        for state in &mut self.temporary_states {
            if state.expired_reported || now < state.expires_at {
                continue;
            }

            eprintln!("edgelog: temporary rule '{}' expired", state.name);
            state.expired_reported = true;
        }
    }
}

fn temporary_groups(rules: &[TemporaryRule]) -> Vec<TemporaryGroup> {
    let mut groups: Vec<TemporaryGroup> = Vec::new();

    for rule in rules {
        let fingerprint_part = format!("{}:{:?}", rule.directive_text, rule.duration);

        match groups.iter_mut().find(|group| group.name == rule.name) {
            Some(group) => {
                if rule.duration > group.duration {
                    group.duration = rule.duration;
                }
                group.fingerprint.push('\n');
                group.fingerprint.push_str(&fingerprint_part);
                group.rules.push(rule.rule.clone());
            }
            None => groups.push(TemporaryGroup {
                name: rule.name.clone(),
                duration: rule.duration,
                fingerprint: fingerprint_part,
                rules: vec![rule.rule.clone()],
            }),
        }
    }

    groups
}

#[derive(Debug)]
struct Args {
    config: PathBuf,
    input: Option<PathBuf>,
    from_end: bool,
    create_config: bool,
    buffers_dir: Option<PathBuf>,
    hops_dir: Option<PathBuf>,
    control_listen: Option<String>,
    control_only: bool,
    node_id: String,
    register_addr: Option<String>,
    prometheus_listen: Option<String>,
}

impl Args {
    fn parse() -> Self {
        let mut args = env::args().skip(1);
        let mut parsed = Self {
            config: env::var_os("EDGLOG_CONFIG")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG)),
            input: env::var_os("EDGLOG_INPUT").map(PathBuf::from),
            from_end: env::var_os("EDGLOG_FROM_END").is_some(),
            create_config: env::var_os("EDGLOG_CREATE_CONFIG").is_some(),
            buffers_dir: env::var_os("EDGLOG_BUFFERS_DIR").map(PathBuf::from),
            hops_dir: env::var_os("EDGLOG_HOPS_DIR").map(PathBuf::from),
            control_listen: env::var("EDGLOG_CONTROL_LISTEN").ok(),
            control_only: env::var_os("EDGLOG_CONTROL_ONLY").is_some(),
            node_id: env::var("EDGLOG_NODE_ID").unwrap_or_else(|_| "local".to_string()),
            register_addr: env::var("EDGLOG_REGISTER_ADDR").ok(),
            prometheus_listen: env::var("EDGLOG_PROMETHEUS_LISTEN")
                .or_else(|_| env::var("EDGLOG_METRICS_LISTEN"))
                .ok(),
        };

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--config" => {
                    parsed.config = PathBuf::from(
                        args.next()
                            .unwrap_or_else(|| usage_exit("--config requires a path")),
                    );
                }
                "--input" => {
                    parsed.input = Some(PathBuf::from(
                        args.next()
                            .unwrap_or_else(|| usage_exit("--input requires a path")),
                    ));
                }
                "--from-end" => parsed.from_end = true,
                "--create-config" => parsed.create_config = true,
                "--buffers-dir" => {
                    parsed.buffers_dir =
                        Some(PathBuf::from(args.next().unwrap_or_else(|| {
                            usage_exit("--buffers-dir requires a path")
                        })));
                }
                "--hops-dir" => {
                    parsed.hops_dir = Some(PathBuf::from(
                        args.next()
                            .unwrap_or_else(|| usage_exit("--hops-dir requires a path")),
                    ));
                }
                "--control-listen" => {
                    parsed.control_listen = Some(
                        args.next()
                            .unwrap_or_else(|| usage_exit("--control-listen requires HOST:PORT")),
                    );
                }
                "--control-only" => parsed.control_only = true,
                "--node-id" => {
                    parsed.node_id = args
                        .next()
                        .unwrap_or_else(|| usage_exit("--node-id requires a name"));
                }
                "--register-addr" => {
                    parsed.register_addr = Some(
                        args.next()
                            .unwrap_or_else(|| usage_exit("--register-addr requires HOST:PORT")),
                    );
                }
                "--prometheus-listen" | "--metrics-listen" => {
                    parsed.prometheus_listen =
                        Some(args.next().unwrap_or_else(|| {
                            usage_exit("--prometheus-listen requires HOST:PORT")
                        }));
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
        eprintln!("edgelog: {message}");
    }

    eprintln!(
        "usage: edgelog [--config /etc/edgelog/filter.conf] [--input /logs/app.log] [--from-end] [--create-config] [--buffers-dir /var/run/edgelog/buffers] [--hops-dir /var/run/edgelog/hops] [--control-listen 127.0.0.1:7777] [--node-id NAME] [--register-addr HOST:PORT] [--prometheus-listen 127.0.0.1:9100] [--control-only]"
    );
    std::process::exit(if message.is_empty() { 0 } else { 2 });
}

fn main() -> io::Result<()> {
    let args = Args::parse();
    let config_path = args.config.clone();
    let register_addr = args
        .register_addr
        .clone()
        .or_else(|| args.control_listen.clone());
    let metrics = Arc::new(Metrics::new(args.node_id.clone())?);
    let traces = Arc::new(Traces::new(args.node_id.clone()));
    let audit = Arc::new(AuditLog::new());

    if args.create_config {
        create_default_config(&args.config)?;
    }

    if let Ok(filter) = Filter::from_file(&config_path) {
        metrics.update_config(&filter.metrics);
        traces.update_config(&filter.traces);
    }

    start_metrics_config_watcher(config_path.clone(), Arc::clone(&metrics));
    start_traces_config_watcher(config_path.clone(), Arc::clone(&traces));

    if let Some(addr) = args.prometheus_listen.clone() {
        start_prometheus_server(addr, Arc::clone(&metrics))?;
    }

    if let Some(addr) = args.control_listen.clone() {
        start_control_server(
            addr,
            args.node_id.clone(),
            config_path.clone(),
            args.buffers_dir.clone(),
            Arc::clone(&metrics),
            Arc::clone(&traces),
            Arc::clone(&audit),
        )?;
    }

    if let Some(addr) = register_addr {
        start_upstream_registrar(config_path.clone(), args.node_id.clone(), addr);
    }

    if args.control_only {
        loop {
            thread::sleep(Duration::from_secs(60));
        }
    }

    let mut filter = LiveFilter::new(args.config);
    let mut rings = RingBuffers::new(args.buffers_dir, Arc::clone(&metrics), Arc::clone(&traces));
    let mut hops = DownstreamHops::new(args.hops_dir, Arc::clone(&metrics), Arc::clone(&traces));
    let mut controls = OutputControls::default();

    match args.input {
        Some(path) => tail_file(
            &path,
            args.from_end,
            &mut filter,
            &mut rings,
            &mut hops,
            &mut controls,
            &metrics,
            &traces,
        ),
        None => filter_stdin(
            &mut filter,
            &mut rings,
            &mut hops,
            &mut controls,
            &metrics,
            &traces,
        ),
    }
}

fn create_default_config(path: &Path) -> io::Result<()> {
    if path.exists() {
        return Ok(());
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::write(
        path,
        "# Edit this file in-place; edgelog reloads it automatically.\n\
         # mode=include keeps matching lines, mode=exclude drops matching lines.\n\
         # sample=10 emits every tenth allowed line.\n\
         # throttle_per_second=50 emits at most 50 allowed lines per second.\n\
         # temp debug-boost 5m clear_patterns temporarily lets all lines through.\n\
         # ring recent-errors 100 ERROR keeps the last 100 lines containing ERROR.\n\
         # hop alerts ERROR appends matching lines to alerts.log in --hops-dir.\n\
         # peer leaf-a 10.0.0.12:7777 lets the control server route to leaf-a.\n\
         # upstream 10.0.0.1:7777 registers this node with a parent server.\n\
         # tunnel admin 127.0.0.1:8080 http exposes a local port only via CONNECT.\n\
         # metrics=on enables Prometheus and StatsD counters.\n\
         # metric_label ring=on temporarily adds ring labels when needed.\n\
         # traces=on enables lightweight JSONL spans.\n\
         # trace_file /var/run/edgelog/spans.jsonl writes spans to a mounted file.\n\
         mode=include\n",
    )
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct MetricKey {
    name: String,
    dims: MetricDims,
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
struct MetricDims {
    node: Option<String>,
    ring: Option<String>,
    hop: Option<String>,
    command: Option<String>,
    outcome: Option<String>,
}

struct Metrics {
    node_id: String,
    config: Mutex<MetricsConfig>,
    counters: Mutex<HashMap<MetricKey, u64>>,
    statsd_socket: UdpSocket,
}

struct Traces {
    node_id: String,
    config: Mutex<TraceConfig>,
    counter: AtomicU64,
    sampled: Mutex<usize>,
}

impl Metrics {
    fn new(node_id: String) -> io::Result<Self> {
        Ok(Self {
            node_id,
            config: Mutex::new(MetricsConfig::default()),
            counters: Mutex::new(HashMap::new()),
            statsd_socket: UdpSocket::bind("0.0.0.0:0")?,
        })
    }

    fn update_config(&self, config: &MetricsConfig) {
        *self
            .config
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = config.clone();
    }

    fn record(&self, config: &MetricsConfig, name: &str, mut dims: MetricDims) {
        self.update_config(config);

        if !config.enabled {
            return;
        }

        if dims.node.is_none() {
            dims.node = Some(self.node_id.clone());
        }

        let key = MetricKey {
            name: name.to_string(),
            dims: dims.clone(),
        };

        *self
            .counters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(key)
            .or_insert(0) += 1;

        if let Some(addr) = &config.statsd_addr {
            let line = self.statsd_line(config, name, &dims);
            let _ = self.statsd_socket.send_to(line.as_bytes(), addr);
        }
    }

    fn render_prometheus(&self) -> String {
        let config = self
            .config
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();

        if !config.enabled {
            return "# edgelog metrics disabled\n".to_string();
        }

        let mut grouped = BTreeMap::<String, u64>::new();

        for (key, value) in self
            .counters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
        {
            let labels = prometheus_labels(&config.labels, &key.dims);
            let metric = prometheus_metric(&key.name, &labels);
            *grouped.entry(metric).or_insert(0) += value;
        }

        let mut output = String::new();
        output.push_str("# TYPE edgelog_input_lines_total counter\n");
        output.push_str("# TYPE edgelog_stdout_lines_total counter\n");
        output.push_str("# TYPE edgelog_output_drops_total counter\n");
        output.push_str("# TYPE edgelog_ring_writes_total counter\n");
        output.push_str("# TYPE edgelog_hop_writes_total counter\n");
        output.push_str("# TYPE edgelog_control_requests_total counter\n");
        output.push_str("# TYPE edgelog_tunnel_connects_total counter\n");
        output.push_str("# TYPE edgelog_ring_evictions_total counter\n");
        output.push_str("# TYPE edgelog_audit_events_total counter\n");
        output.push_str("# TYPE edgelog_audit_evictions_total counter\n");

        for (metric, value) in grouped {
            output.push_str(&metric);
            output.push(' ');
            output.push_str(&value.to_string());
            output.push('\n');
        }

        output
    }

    fn statsd_line(&self, config: &MetricsConfig, name: &str, dims: &MetricDims) -> String {
        let mut line = format!("{}.{}:1|c", config.statsd_prefix, name);
        let tags = statsd_tags(&config.labels, dims);

        if !tags.is_empty() {
            line.push_str("|#");
            line.push_str(&tags.join(","));
        }

        line
    }
}

impl Traces {
    fn new(node_id: String) -> Self {
        Self {
            node_id,
            config: Mutex::new(TraceConfig::default()),
            counter: AtomicU64::new(1),
            sampled: Mutex::new(0),
        }
    }

    fn update_config(&self, config: &TraceConfig) {
        *self
            .config
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = config.clone();
    }

    fn record(
        &self,
        config: &TraceConfig,
        name: &str,
        started_at: Instant,
        attrs: Vec<(&str, String)>,
    ) {
        self.update_config(config);

        if !config.enabled || !self.should_sample(config) {
            return;
        }

        let seq = self.counter.fetch_add(1, Ordering::Relaxed);
        let trace_id = format!("{:016x}{seq:016x}", unix_nanos());
        let span_id = format!("{seq:016x}");
        let span = trace_json(
            &trace_id,
            &span_id,
            &self.node_id,
            name,
            started_at.elapsed().as_micros(),
            attrs,
        );

        if let Some(path) = &config.file_path {
            if let Some(parent) = path.parent() {
                let _ = fs::create_dir_all(parent);
            }

            if let Ok(mut file) = fs::OpenOptions::new().create(true).append(true).open(path) {
                let _ = writeln!(file, "{span}");
            }
        }

        if let Some(addr) = &config.tcp_addr {
            if let Ok(mut stream) = TcpStream::connect(addr) {
                let _ = writeln!(stream, "{span}");
            }
        }
    }

    fn should_sample(&self, config: &TraceConfig) -> bool {
        let mut sampled = self
            .sampled
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *sampled += 1;
        *sampled % config.sample_every == 0
    }
}

fn start_metrics_config_watcher(config_path: PathBuf, metrics: Arc<Metrics>) {
    thread::spawn(move || {
        let mut filter = LiveFilter::new(config_path);

        loop {
            let snapshot = filter.snapshot();
            metrics.update_config(&snapshot.metrics);
            thread::sleep(POLL_INTERVAL);
        }
    });
}

fn start_traces_config_watcher(config_path: PathBuf, traces: Arc<Traces>) {
    thread::spawn(move || {
        let mut filter = LiveFilter::new(config_path);

        loop {
            let snapshot = filter.snapshot();
            traces.update_config(&snapshot.traces);
            thread::sleep(POLL_INTERVAL);
        }
    });
}

fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

fn trace_json(
    trace_id: &str,
    span_id: &str,
    node_id: &str,
    name: &str,
    duration_us: u128,
    attrs: Vec<(&str, String)>,
) -> String {
    let mut fields = vec![
        format!("\"trace_id\":\"{}\"", escape_json(trace_id)),
        format!("\"span_id\":\"{}\"", escape_json(span_id)),
        format!("\"node\":\"{}\"", escape_json(node_id)),
        format!("\"name\":\"{}\"", escape_json(name)),
        format!("\"start_unix_nanos\":{}", unix_nanos()),
        format!("\"duration_us\":{}", duration_us),
    ];

    let attrs = attrs
        .into_iter()
        .map(|(name, value)| format!("\"{}\":\"{}\"", escape_json(name), escape_json(&value)))
        .collect::<Vec<_>>()
        .join(",");
    fields.push(format!("\"attrs\":{{{attrs}}}"));

    format!("{{{}}}", fields.join(","))
}

fn escape_json(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn start_prometheus_server(addr: String, metrics: Arc<Metrics>) -> io::Result<()> {
    let listener = TcpListener::bind(&addr)?;
    eprintln!("edgelog: prometheus metrics listening on {addr}");

    thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let metrics = Arc::clone(&metrics);
                    thread::spawn(move || {
                        if let Err(error) = handle_prometheus_client(stream, metrics) {
                            eprintln!("edgelog: prometheus client error: {error}");
                        }
                    });
                }
                Err(error) => eprintln!("edgelog: prometheus accept error: {error}"),
            }
        }
    });

    Ok(())
}

fn handle_prometheus_client(mut stream: TcpStream, metrics: Arc<Metrics>) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request = String::new();
    reader.read_line(&mut request)?;

    let body = if request.starts_with("GET /metrics ") || request.starts_with("GET / ") {
        metrics.render_prometheus()
    } else {
        "not found\n".to_string()
    };
    let status = if request.starts_with("GET /metrics ") || request.starts_with("GET / ") {
        "200 OK"
    } else {
        "404 Not Found"
    };

    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )?;

    Ok(())
}

fn prometheus_labels(labels: &MetricLabels, dims: &MetricDims) -> Vec<(&'static str, String)> {
    let mut out = Vec::new();

    if labels.node {
        push_label(&mut out, "node", &dims.node);
    }
    if labels.ring {
        push_label(&mut out, "ring", &dims.ring);
    }
    if labels.hop {
        push_label(&mut out, "hop", &dims.hop);
    }
    if labels.command {
        push_label(&mut out, "command", &dims.command);
    }
    if labels.outcome {
        push_label(&mut out, "outcome", &dims.outcome);
    }

    out
}

fn push_label(out: &mut Vec<(&'static str, String)>, name: &'static str, value: &Option<String>) {
    if let Some(value) = value {
        out.push((name, value.clone()));
    }
}

fn prometheus_metric(name: &str, labels: &[(&'static str, String)]) -> String {
    if labels.is_empty() {
        return name.to_string();
    }

    let encoded = labels
        .iter()
        .map(|(name, value)| format!("{name}=\"{}\"", escape_prometheus_label(value)))
        .collect::<Vec<_>>()
        .join(",");

    format!("{name}{{{encoded}}}")
}

fn escape_prometheus_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

fn statsd_tags(labels: &MetricLabels, dims: &MetricDims) -> Vec<String> {
    let mut out = Vec::new();

    if labels.node {
        push_statsd_tag(&mut out, "node", &dims.node);
    }
    if labels.ring {
        push_statsd_tag(&mut out, "ring", &dims.ring);
    }
    if labels.hop {
        push_statsd_tag(&mut out, "hop", &dims.hop);
    }
    if labels.command {
        push_statsd_tag(&mut out, "command", &dims.command);
    }
    if labels.outcome {
        push_statsd_tag(&mut out, "outcome", &dims.outcome);
    }

    out
}

fn push_statsd_tag(out: &mut Vec<String>, name: &str, value: &Option<String>) {
    if let Some(value) = value {
        out.push(format!("{name}:{}", sanitize_statsd_tag(value)));
    }
}

fn sanitize_statsd_tag(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            ',' | '|' | ':' | '\n' | '\r' => '_',
            _ => ch,
        })
        .collect()
}

#[derive(Default)]
struct AuditLog {
    inner: Mutex<AuditState>,
}

#[derive(Default)]
struct AuditState {
    seq: u64,
    chain_head: String,
    master: ByteRing,
    payload: ByteRing,
    sessions: VecDeque<AuditSessionSummary>,
    master_evictions: u64,
    payload_evictions: u64,
    session_evictions: u64,
}

#[derive(Clone, Debug)]
struct AuditSessionSummary {
    id: u64,
    peer: String,
    target: String,
    kind: TunnelKind,
    opened_ms: u128,
    closed_ms: Option<u128>,
    bytes_in: u64,
    bytes_out: u64,
    payload_events: u64,
    status: String,
}

#[derive(Default)]
struct ByteRing {
    entries: VecDeque<String>,
    bytes: usize,
    evictions: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct AuditReport {
    master_evictions: u64,
    payload_evictions: u64,
    session_evictions: u64,
}

impl AuditReport {
    fn has_evictions(self) -> bool {
        self.master_evictions > 0 || self.payload_evictions > 0 || self.session_evictions > 0
    }
}

impl AuditLog {
    fn new() -> Self {
        Self::default()
    }

    fn open_session(&self, peer: String, tunnel: &TunnelConfig) -> (u64, AuditReport) {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let id = state.seq + 1;
        let opened_ms = unix_millis();
        let summary = AuditSessionSummary {
            id,
            peer,
            target: tunnel.name.clone(),
            kind: tunnel.kind,
            opened_ms,
            closed_ms: None,
            bytes_in: 0,
            bytes_out: 0,
            payload_events: 0,
            status: "open".to_string(),
        };
        state.sessions.push_back(summary);
        let mut report = state.enforce_session_limit();
        let event = format!(
            "session_open id={id} peer={} target={} kind={} opened_ms={opened_ms}",
            escape_audit_field(
                &state
                    .sessions
                    .back()
                    .map(|session| session.peer.clone())
                    .unwrap_or_default()
            ),
            escape_audit_field(&tunnel.name),
            tunnel.kind.as_str()
        );
        report.add(state.push_master_event(&event));
        (id, report)
    }

    fn record_payload(&self, session_id: u64, direction: AuditDirection, data: &[u8]) -> AuditReport {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(session) = state.sessions.iter_mut().find(|session| session.id == session_id) {
            match direction {
                AuditDirection::Input => session.bytes_in += data.len() as u64,
                AuditDirection::Output => session.bytes_out += data.len() as u64,
            }
            session.payload_events += 1;
        }

        let preview_len = data.len().min(AUDIT_PREVIEW_BYTES);
        let preview = escape_audit_field(&String::from_utf8_lossy(&data[..preview_len]));
        let event = format!(
            "payload id={session_id} direction={} bytes={} preview={}{}",
            direction.as_str(),
            data.len(),
            preview,
            if data.len() > preview_len { " truncated=true" } else { "" }
        );
        state.push_payload_event(event)
    }

    fn close_session(
        &self,
        session_id: u64,
        bytes_in: u64,
        bytes_out: u64,
        status: &str,
    ) -> AuditReport {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let closed_ms = unix_millis();
        let mut target = String::new();
        if let Some(session) = state.sessions.iter_mut().find(|session| session.id == session_id) {
            session.closed_ms = Some(closed_ms);
            session.bytes_in = bytes_in;
            session.bytes_out = bytes_out;
            session.status = status.to_string();
            target = session.target.clone();
        }
        state.push_master_event(&format!(
            "session_close id={session_id} target={} status={} closed_ms={closed_ms} bytes_in={bytes_in} bytes_out={bytes_out}",
            escape_audit_field(&target),
            escape_audit_field(status)
        ))
    }

    fn write_control(&self, command: &str, stream: &mut TcpStream) -> io::Result<()> {
        let state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut parts = command.split_whitespace();
        let _audit = parts.next();

        match parts.next() {
            None | Some("SUMMARY") => state.write_summary(stream),
            Some("MASTER") => state.write_master(stream),
            Some("SESSIONS") => state.write_sessions(stream),
            Some("SESSION") => {
                let Some(id) = parts.next().and_then(|value| value.parse().ok()) else {
                    writeln!(stream, "ERR AUDIT SESSION requires an id")?;
                    return Ok(());
                };
                state.write_session(stream, id)
            }
            Some(other) => {
                writeln!(stream, "ERR unknown AUDIT command {other}")?;
                Ok(())
            }
        }
    }
}

impl AuditState {
    fn push_master_event(&mut self, event: &str) -> AuditReport {
        self.seq += 1;
        let hash = audit_hash(&self.chain_head, self.seq, event);
        self.chain_head = hash.clone();
        let line = format!("seq={} hash={} {}", self.seq, hash, event);
        let evicted = self.master.push(line, AUDIT_MASTER_BUDGET_BYTES);
        self.master_evictions += evicted;
        AuditReport {
            master_evictions: evicted,
            ..AuditReport::default()
        }
    }

    fn push_payload_event(&mut self, event: String) -> AuditReport {
        self.seq += 1;
        let hash = audit_hash(&self.chain_head, self.seq, &event);
        self.chain_head = hash.clone();
        let line = format!("seq={} hash={} {}", self.seq, hash, event);
        let payload_evictions = self.payload.push(line, AUDIT_PAYLOAD_BUDGET_BYTES);
        self.payload_evictions += payload_evictions;
        let mut report = AuditReport {
            payload_evictions,
            ..AuditReport::default()
        };

        if payload_evictions > 0 {
            report.add(self.push_master_event(&format!(
                "payload_overflow evicted={} payload_evictions_total={}",
                payload_evictions, self.payload_evictions
            )));
        }

        report
    }

    fn enforce_session_limit(&mut self) -> AuditReport {
        let mut report = AuditReport::default();

        while self.sessions.len() > AUDIT_MAX_SESSIONS {
            let evicted = self
                .sessions
                .iter()
                .position(|session| session.closed_ms.is_some())
                .unwrap_or(0);
            let Some(session) = self.sessions.remove(evicted) else {
                break;
            };
            self.session_evictions += 1;
            report.session_evictions += 1;
            report.add(self.push_master_event(&format!(
                "session_summary_evicted id={} target={} session_evictions_total={}",
                session.id,
                escape_audit_field(&session.target),
                self.session_evictions
            )));
        }

        report
    }

    fn write_summary(&self, stream: &mut TcpStream) -> io::Result<()> {
        writeln!(stream, "OK")?;
        writeln!(stream, "audit_total_budget_bytes {AUDIT_TOTAL_BUDGET_BYTES}")?;
        writeln!(stream, "audit_master_budget_bytes {AUDIT_MASTER_BUDGET_BYTES}")?;
        writeln!(stream, "audit_payload_budget_bytes {AUDIT_PAYLOAD_BUDGET_BYTES}")?;
        writeln!(stream, "chain_head {}", self.chain_head)?;
        writeln!(stream, "master_bytes {}", self.master.bytes)?;
        writeln!(stream, "payload_bytes {}", self.payload.bytes)?;
        writeln!(stream, "master_evictions {}", self.master_evictions)?;
        writeln!(stream, "payload_evictions {}", self.payload_evictions)?;
        writeln!(stream, "session_evictions {}", self.session_evictions)?;
        writeln!(stream, "sessions {}", self.sessions.len())?;
        writeln!(stream, "END")?;
        Ok(())
    }

    fn write_master(&self, stream: &mut TcpStream) -> io::Result<()> {
        writeln!(stream, "OK")?;
        for entry in &self.master.entries {
            writeln!(stream, "{entry}")?;
        }
        writeln!(stream, "END")?;
        Ok(())
    }

    fn write_sessions(&self, stream: &mut TcpStream) -> io::Result<()> {
        writeln!(stream, "OK")?;
        for session in &self.sessions {
            writeln!(
                stream,
                "id={} peer={} target={} kind={} opened_ms={} closed_ms={} status={} bytes_in={} bytes_out={} payload_events={}",
                session.id,
                escape_audit_field(&session.peer),
                escape_audit_field(&session.target),
                session.kind.as_str(),
                session.opened_ms,
                session
                    .closed_ms
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                escape_audit_field(&session.status),
                session.bytes_in,
                session.bytes_out,
                session.payload_events
            )?;
        }
        writeln!(stream, "END")?;
        Ok(())
    }

    fn write_session(&self, stream: &mut TcpStream, id: u64) -> io::Result<()> {
        let Some(session) = self.sessions.iter().find(|session| session.id == id) else {
            writeln!(stream, "ERR audit session not found")?;
            return Ok(());
        };

        writeln!(stream, "OK")?;
        writeln!(
            stream,
            "id={} peer={} target={} kind={} opened_ms={} closed_ms={} status={} bytes_in={} bytes_out={} payload_events={}",
            session.id,
            escape_audit_field(&session.peer),
            escape_audit_field(&session.target),
            session.kind.as_str(),
            session.opened_ms,
            session
                .closed_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            escape_audit_field(&session.status),
            session.bytes_in,
            session.bytes_out,
            session.payload_events
        )?;
        for entry in self
            .payload
            .entries
            .iter()
            .filter(|entry| entry.contains(&format!("id={id} ")))
        {
            writeln!(stream, "{entry}")?;
        }
        writeln!(stream, "END")?;
        Ok(())
    }
}

impl AuditReport {
    fn add(&mut self, other: AuditReport) {
        self.master_evictions += other.master_evictions;
        self.payload_evictions += other.payload_evictions;
        self.session_evictions += other.session_evictions;
    }
}

impl ByteRing {
    fn push(&mut self, mut entry: String, budget: usize) -> u64 {
        let max_entry_bytes = budget.saturating_sub(1);
        if entry.len() > max_entry_bytes {
            entry = bounded_ring_line(&entry, budget);
        }

        let entry_bytes = ring_line_bytes(&entry);
        let mut evicted = 0;

        while !self.entries.is_empty() && self.bytes.saturating_add(entry_bytes) > budget {
            if let Some(old) = self.entries.pop_front() {
                self.bytes = self.bytes.saturating_sub(ring_line_bytes(&old));
                self.evictions += 1;
                evicted += 1;
            }
        }

        self.bytes = self.bytes.saturating_add(entry_bytes);
        self.entries.push_back(entry);
        evicted
    }
}

#[derive(Clone, Copy)]
enum AuditDirection {
    Input,
    Output,
}

impl AuditDirection {
    fn as_str(self) -> &'static str {
        match self {
            AuditDirection::Input => "in",
            AuditDirection::Output => "out",
        }
    }
}

fn audit_hash(previous: &str, seq: u64, event: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(previous.as_bytes());
    hasher.update(seq.to_le_bytes());
    hasher.update(event.as_bytes());
    hex_lower(&hasher.finalize())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn escape_audit_field(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            ' ' | '\n' | '\r' | '\t' => '_',
            _ => ch,
        })
        .collect()
}

fn record_audit_report(metrics: &Metrics, config: &MetricsConfig, report: AuditReport) {
    metrics.record(
        config,
        "edgelog_audit_events_total",
        MetricDims {
            outcome: Some("recorded".to_string()),
            ..MetricDims::default()
        },
    );

    if !report.has_evictions() {
        return;
    }

    for (name, count) in [
        ("master", report.master_evictions),
        ("payload", report.payload_evictions),
        ("session", report.session_evictions),
    ] {
        for _ in 0..count {
            metrics.record(
                config,
                "edgelog_audit_evictions_total",
                MetricDims {
                    command: Some(name.to_string()),
                    outcome: Some("evicted".to_string()),
                    ..MetricDims::default()
                },
            );
        }
    }
}

fn start_control_server(
    addr: String,
    node_id: String,
    config_path: PathBuf,
    buffers_dir: Option<PathBuf>,
    metrics: Arc<Metrics>,
    traces: Arc<Traces>,
    audit: Arc<AuditLog>,
) -> io::Result<()> {
    let listener = TcpListener::bind(&addr)?;
    let registry = Arc::new(Mutex::new(HashMap::<String, String>::new()));

    eprintln!("edgelog: control server listening on {addr} as {node_id}");

    thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let node_id = node_id.clone();
                    let config_path = config_path.clone();
                    let buffers_dir = buffers_dir.clone();
                    let registry = Arc::clone(&registry);
                    let metrics = Arc::clone(&metrics);
                    let traces = Arc::clone(&traces);
                    let audit = Arc::clone(&audit);

                    thread::spawn(move || {
                        if let Err(error) = handle_control_client(
                            stream,
                            &node_id,
                            &config_path,
                            &buffers_dir,
                            registry,
                            metrics,
                            traces,
                            audit,
                        ) {
                            eprintln!("edgelog: control client error: {error}");
                        }
                    });
                }
                Err(error) => eprintln!("edgelog: control accept error: {error}"),
            }
        }
    });

    Ok(())
}

fn handle_control_client(
    mut stream: TcpStream,
    node_id: &str,
    config_path: &Path,
    buffers_dir: &Option<PathBuf>,
    registry: Arc<Mutex<HashMap<String, String>>>,
    metrics: Arc<Metrics>,
    traces: Arc<Traces>,
    audit: Arc<AuditLog>,
) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut command = String::new();

    if reader.read_line(&mut command)? == 0 {
        return Ok(());
    }

    handle_control_command(
        command.trim_end_matches(['\r', '\n']),
        &mut stream,
        node_id,
        config_path,
        buffers_dir,
        registry,
        metrics,
        traces,
        audit,
    )
}

fn handle_control_command(
    command: &str,
    stream: &mut TcpStream,
    node_id: &str,
    config_path: &Path,
    buffers_dir: &Option<PathBuf>,
    registry: Arc<Mutex<HashMap<String, String>>>,
    metrics: Arc<Metrics>,
    traces: Arc<Traces>,
    audit: Arc<AuditLog>,
) -> io::Result<()> {
    let started_at = Instant::now();
    let command_name = command.split_whitespace().next().unwrap_or("EMPTY");
    let telemetry_config = Filter::from_file(config_path).unwrap_or_default();
    metrics.record(
        &telemetry_config.metrics,
        "edgelog_control_requests_total",
        MetricDims {
            command: Some(command_name.to_string()),
            outcome: Some("received".to_string()),
            ..MetricDims::default()
        },
    );
    traces.record(
        &telemetry_config.traces,
        "control.request",
        started_at,
        vec![("command", command_name.to_string())],
    );

    if command == "PING" {
        writeln!(stream, "OK {node_id}")?;
        return Ok(());
    }

    if command == "PEERS" {
        writeln!(stream, "OK")?;
        for peer in control_peers(config_path, &registry) {
            writeln!(stream, "{} {}", peer.name, peer.addr)?;
        }
        writeln!(stream, "END")?;
        return Ok(());
    }

    if command == "RINGS" {
        return write_ring_list(stream, buffers_dir);
    }

    if command == "TUNNELS" {
        return write_tunnel_list(stream, config_path);
    }

    if command == "AUDIT" || command.starts_with("AUDIT ") {
        return audit.write_control(command, stream);
    }

    if let Some(rest) = command.strip_prefix("REGISTER ") {
        let mut parts = rest.splitn(2, char::is_whitespace);
        let name = parts.next().unwrap_or("").trim();
        let addr = parts.next().unwrap_or("").trim();

        if name.is_empty() || name.contains('/') || addr.is_empty() {
            writeln!(stream, "ERR REGISTER requires NAME HOST:PORT")?;
            return Ok(());
        }

        registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(name.to_string(), addr.to_string());
        writeln!(stream, "OK registered {name}")?;
        return Ok(());
    }

    if let Some(rest) = command.strip_prefix("TAIL ") {
        let (ring, lines) = parse_tail_args(rest);
        return write_ring_tail(stream, buffers_dir, ring, lines);
    }

    if let Some(rest) = command.strip_prefix("FOLLOW ") {
        let (ring, lines) = parse_tail_args(rest);
        return follow_ring(stream, buffers_dir, ring, lines);
    }

    if let Some(rest) = command.strip_prefix("CONNECT ") {
        let target = rest.trim();
        return connect_tunnel(
            stream,
            config_path,
            target,
            &telemetry_config,
            metrics,
            traces,
            audit,
        );
    }

    if let Some(rest) = command.strip_prefix("ROUTE ") {
        let mut parts = rest.splitn(2, char::is_whitespace);
        let peer = parts.next().unwrap_or("").trim();
        let downstream_command = parts.next().unwrap_or("").trim();

        if peer.is_empty() || downstream_command.is_empty() {
            writeln!(stream, "ERR ROUTE requires PEER COMMAND")?;
            return Ok(());
        }

        return route_control_command(stream, config_path, &registry, peer, downstream_command);
    }

    writeln!(stream, "ERR unknown command")?;
    Ok(())
}

fn start_upstream_registrar(config_path: PathBuf, node_id: String, register_addr: String) {
    thread::spawn(move || {
        loop {
            match Filter::from_file(&config_path) {
                Ok(filter) => {
                    for upstream in filter.upstreams {
                        if let Err(error) =
                            register_with_upstream(&upstream.addr, &node_id, &register_addr)
                        {
                            eprintln!(
                                "edgelog: could not register with upstream {}: {error}",
                                upstream.addr
                            );
                        }
                    }
                }
                Err(error) => eprintln!(
                    "edgelog: could not load upstream config {}: {error}",
                    config_path.display()
                ),
            }

            thread::sleep(Duration::from_secs(5));
        }
    });
}

fn register_with_upstream(upstream: &str, node_id: &str, register_addr: &str) -> io::Result<()> {
    let mut stream = TcpStream::connect(upstream)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    writeln!(stream, "REGISTER {node_id} {register_addr}")?;

    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response)?;
    Ok(())
}

fn route_control_command(
    stream: &mut TcpStream,
    config_path: &Path,
    registry: &Arc<Mutex<HashMap<String, String>>>,
    peer: &str,
    downstream_command: &str,
) -> io::Result<()> {
    let Some(addr) = lookup_control_peer(config_path, registry, peer) else {
        writeln!(stream, "ERR unknown peer {peer}")?;
        return Ok(());
    };

    let mut downstream = TcpStream::connect(addr)?;
    downstream.write_all(downstream_command.as_bytes())?;
    downstream.write_all(b"\n")?;

    if control_command_is_stream(downstream_command) {
        return bridge_tcp_streams(stream, downstream, None);
    }

    io::copy(&mut downstream, stream)?;
    Ok(())
}

fn control_command_is_stream(command: &str) -> bool {
    let command = command.trim_start();

    if command.starts_with("CONNECT ") {
        return true;
    }

    let Some(rest) = command.strip_prefix("ROUTE ") else {
        return false;
    };

    let mut parts = rest.splitn(2, char::is_whitespace);
    let _peer = parts.next();
    parts.next().map(control_command_is_stream).unwrap_or(false)
}

fn write_tunnel_list(stream: &mut TcpStream, config_path: &Path) -> io::Result<()> {
    let filter = match Filter::from_file(config_path) {
        Ok(filter) => filter,
        Err(error) => {
            writeln!(stream, "ERR could not load config: {error}")?;
            return Ok(());
        }
    };

    writeln!(stream, "OK")?;
    for tunnel in filter.tunnels {
        writeln!(
            stream,
            "{} {} {}",
            tunnel.name,
            tunnel.addr,
            tunnel.kind.as_str()
        )?;
    }
    writeln!(stream, "END")?;
    Ok(())
}

fn connect_tunnel(
    stream: &mut TcpStream,
    config_path: &Path,
    target: &str,
    telemetry_filter: &Filter,
    metrics: Arc<Metrics>,
    traces: Arc<Traces>,
    audit: Arc<AuditLog>,
) -> io::Result<()> {
    let started_at = Instant::now();

    if !is_safe_control_name(target) {
        writeln!(stream, "ERR CONNECT requires a configured tunnel name")?;
        record_tunnel_connect(&metrics, &telemetry_filter.metrics, "invalid");
        return Ok(());
    }

    let filter = match Filter::from_file(config_path) {
        Ok(filter) => filter,
        Err(error) => {
            writeln!(stream, "ERR could not load config: {error}")?;
            record_tunnel_connect(&metrics, &telemetry_filter.metrics, "config_error");
            return Ok(());
        }
    };

    let Some(tunnel) = filter
        .tunnels
        .into_iter()
        .find(|tunnel| tunnel.name == target)
    else {
        writeln!(stream, "ERR unknown tunnel {target}")?;
        record_tunnel_connect(&metrics, &telemetry_filter.metrics, "unknown");
        return Ok(());
    };

    let local = match TcpStream::connect(&tunnel.addr) {
        Ok(local) => local,
        Err(error) => {
            writeln!(
                stream,
                "ERR could not connect tunnel {}: {error}",
                tunnel.name
            )?;
            record_tunnel_connect(&metrics, &telemetry_filter.metrics, "connect_error");
            traces.record(
                &telemetry_filter.traces,
                "tunnel.connect",
                started_at,
                vec![
                    ("tunnel", tunnel.name),
                    ("kind", tunnel.kind.as_str().to_string()),
                    ("outcome", "connect_error".to_string()),
                ],
            );
            return Ok(());
        }
    };

    let _ = stream.set_nodelay(true);
    let _ = local.set_nodelay(true);
    writeln!(
        stream,
        "OK connected {} {} {}",
        tunnel.name,
        tunnel.addr,
        tunnel.kind.as_str()
    )?;
    stream.flush()?;
    record_tunnel_connect(&metrics, &telemetry_filter.metrics, "connected");
    traces.record(
        &telemetry_filter.traces,
        "tunnel.connect",
        started_at,
        vec![
            ("tunnel", tunnel.name.clone()),
            ("kind", tunnel.kind.as_str().to_string()),
            ("outcome", "connected".to_string()),
        ],
    );

    let peer = stream
        .peer_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let (audit_session_id, report) = audit.open_session(peer, &tunnel);
    record_audit_report(&metrics, &telemetry_filter.metrics, report);

    start_tunnel_detach_watcher(
        config_path.to_path_buf(),
        tunnel.name.clone(),
        tunnel.addr.clone(),
        tunnel.kind,
        stream.try_clone()?,
        local.try_clone()?,
    );
    bridge_tcp_streams(
        stream,
        local,
        Some(AuditBridge {
            audit,
            metrics,
            metrics_config: telemetry_filter.metrics.clone(),
            session_id: audit_session_id,
        }),
    )
}

fn record_tunnel_connect(metrics: &Metrics, config: &MetricsConfig, outcome: &str) {
    metrics.record(
        config,
        "edgelog_tunnel_connects_total",
        MetricDims {
            command: Some("CONNECT".to_string()),
            outcome: Some(outcome.to_string()),
            ..MetricDims::default()
        },
    );
}

fn start_tunnel_detach_watcher(
    config_path: PathBuf,
    name: String,
    addr: String,
    kind: TunnelKind,
    control_stream: TcpStream,
    target_stream: TcpStream,
) {
    thread::spawn(move || {
        loop {
            thread::sleep(POLL_INTERVAL);

            let Ok(filter) = Filter::from_file(&config_path) else {
                continue;
            };

            let still_attached = filter
                .tunnels
                .iter()
                .any(|tunnel| tunnel.name == name && tunnel.addr == addr && tunnel.kind == kind);

            if still_attached {
                continue;
            }

            eprintln!("edgelog: tunnel {name} detached");
            let _ = control_stream.shutdown(Shutdown::Both);
            let _ = target_stream.shutdown(Shutdown::Both);
            break;
        }
    });
}

#[derive(Clone)]
struct AuditBridge {
    audit: Arc<AuditLog>,
    metrics: Arc<Metrics>,
    metrics_config: MetricsConfig,
    session_id: u64,
}

fn bridge_tcp_streams(
    left: &mut TcpStream,
    right: TcpStream,
    audit: Option<AuditBridge>,
) -> io::Result<()> {
    let mut left_reader = left.try_clone()?;
    let mut left_writer = left.try_clone()?;
    let mut right_reader = right.try_clone()?;
    let mut right_writer = right;
    let audit_in = audit.clone();
    let audit_out = audit.clone();

    let left_to_right = thread::spawn(move || {
        let result = copy_with_audit(
            &mut left_reader,
            &mut right_writer,
            audit_in.as_ref(),
            AuditDirection::Input,
        );
        let _ = right_writer.shutdown(Shutdown::Write);
        result
    });
    let right_to_left = thread::spawn(move || {
        let result = copy_with_audit(
            &mut right_reader,
            &mut left_writer,
            audit_out.as_ref(),
            AuditDirection::Output,
        );
        let _ = left_writer.shutdown(Shutdown::Write);
        result
    });

    let bytes_in = join_bridge_copy(left_to_right)?;
    let bytes_out = join_bridge_copy(right_to_left)?;

    if let Some(audit) = audit {
        let report = audit
            .audit
            .close_session(audit.session_id, bytes_in, bytes_out, "closed");
        record_audit_report(&audit.metrics, &audit.metrics_config, report);
    }

    Ok(())
}

fn join_bridge_copy(handle: thread::JoinHandle<io::Result<u64>>) -> io::Result<u64> {
    handle
        .join()
        .unwrap_or_else(|_| Err(io::Error::other("tunnel copy thread panicked")))
}

fn copy_with_audit(
    reader: &mut TcpStream,
    writer: &mut TcpStream,
    audit: Option<&AuditBridge>,
    direction: AuditDirection,
) -> io::Result<u64> {
    let mut buffer = [0_u8; TUNNEL_COPY_BUFFER_BYTES];
    let mut total = 0_u64;

    loop {
        let read = reader.read(&mut buffer)?;

        if read == 0 {
            return Ok(total);
        }

        writer.write_all(&buffer[..read])?;
        total += read as u64;

        if let Some(audit) = audit {
            let report = audit
                .audit
                .record_payload(audit.session_id, direction, &buffer[..read]);
            record_audit_report(&audit.metrics, &audit.metrics_config, report);
        }
    }
}

fn lookup_control_peer(
    config_path: &Path,
    registry: &Arc<Mutex<HashMap<String, String>>>,
    name: &str,
) -> Option<String> {
    if let Some(addr) = registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(name)
        .cloned()
    {
        return Some(addr);
    }

    Filter::from_file(config_path)
        .ok()?
        .peers
        .into_iter()
        .find(|peer| peer.name == name)
        .map(|peer| peer.addr)
}

fn control_peers(
    config_path: &Path,
    registry: &Arc<Mutex<HashMap<String, String>>>,
) -> Vec<PeerConfig> {
    let mut peers = Filter::from_file(config_path)
        .map(|filter| filter.peers)
        .unwrap_or_default();

    for (name, addr) in registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
    {
        if let Some(peer) = peers.iter_mut().find(|peer| peer.name == *name) {
            peer.addr = addr.clone();
        } else {
            peers.push(PeerConfig {
                name: name.clone(),
                addr: addr.clone(),
            });
        }
    }

    peers
}

fn write_ring_list(stream: &mut TcpStream, buffers_dir: &Option<PathBuf>) -> io::Result<()> {
    let Some(dir) = buffers_dir else {
        writeln!(stream, "ERR --buffers-dir is not configured")?;
        return Ok(());
    };

    writeln!(stream, "OK")?;

    match fs::read_dir(dir) {
        Ok(entries) => {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|ext| ext.to_str()) != Some("log") {
                    continue;
                }

                if let Some(name) = path.file_stem().and_then(|name| name.to_str()) {
                    writeln!(stream, "{name}")?;
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            writeln!(stream, "ERR could not read ring dir: {error}")?;
            return Ok(());
        }
    }

    writeln!(stream, "END")?;
    Ok(())
}

fn parse_tail_args(rest: &str) -> (&str, usize) {
    let mut parts = rest.split_whitespace();
    let ring = parts.next().unwrap_or("");
    let lines = parts
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100);

    (ring, lines)
}

fn write_ring_tail(
    stream: &mut TcpStream,
    buffers_dir: &Option<PathBuf>,
    ring: &str,
    lines: usize,
) -> io::Result<()> {
    let Some(path) = ring_file_path(buffers_dir, ring, false) else {
        writeln!(
            stream,
            "ERR invalid ring or --buffers-dir is not configured"
        )?;
        return Ok(());
    };

    let ring_lines = match read_ring_lines(&path) {
        Ok(lines) => lines,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            writeln!(stream, "ERR ring not found")?;
            return Ok(());
        }
        Err(error) => {
            writeln!(stream, "ERR could not read ring: {error}")?;
            return Ok(());
        }
    };

    writeln!(stream, "OK")?;
    for line in last_lines(&ring_lines, lines) {
        writeln!(stream, "{line}")?;
    }
    writeln!(stream, "END")?;
    Ok(())
}

fn follow_ring(
    stream: &mut TcpStream,
    buffers_dir: &Option<PathBuf>,
    ring: &str,
    lines: usize,
) -> io::Result<()> {
    let Some(path) = ring_file_path(buffers_dir, ring, false) else {
        writeln!(
            stream,
            "ERR invalid ring or --buffers-dir is not configured"
        )?;
        return Ok(());
    };

    writeln!(stream, "OK")?;
    stream.flush()?;

    let mut previous: Vec<String> = Vec::new();

    loop {
        match read_ring_lines(&path) {
            Ok(current) => {
                let visible = last_lines(&current, lines);
                let new_lines = new_visible_lines(&previous, &visible);

                for line in new_lines {
                    writeln!(stream, "{line}")?;
                }

                stream.flush()?;
                previous = visible;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                writeln!(stream, "ERR could not read ring: {error}")?;
                return Ok(());
            }
        }

        thread::sleep(POLL_INTERVAL);
    }
}

fn ring_file_path(buffers_dir: &Option<PathBuf>, ring: &str, create_dir: bool) -> Option<PathBuf> {
    let dir = buffers_dir.as_ref()?;

    if ring.is_empty() || ring.contains('/') || ring == "." || ring == ".." {
        return None;
    }

    if create_dir {
        fs::create_dir_all(dir).ok()?;
    }

    Some(dir.join(format!("{ring}.log")))
}

fn read_ring_lines(path: &Path) -> io::Result<Vec<String>> {
    Ok(fs::read_to_string(path)?
        .lines()
        .map(str::to_string)
        .collect())
}

fn last_lines(lines: &[String], count: usize) -> Vec<String> {
    let start = lines.len().saturating_sub(count);
    lines[start..].to_vec()
}

fn new_visible_lines(previous: &[String], current: &[String]) -> Vec<String> {
    if previous.is_empty() {
        return current.to_vec();
    }

    if current.starts_with(previous) {
        return current[previous.len()..].to_vec();
    }

    current.to_vec()
}

struct RingBuffers {
    dir: Option<PathBuf>,
    buffers: Vec<NamedRing>,
    metrics: Arc<Metrics>,
    traces: Arc<Traces>,
}

struct NamedRing {
    config: RingConfig,
    lines: VecDeque<String>,
    bytes: usize,
    evictions: u64,
}

impl RingBuffers {
    fn new(dir: Option<PathBuf>, metrics: Arc<Metrics>, traces: Arc<Traces>) -> Self {
        Self {
            dir,
            buffers: Vec::new(),
            metrics,
            traces,
        }
    }

    fn observe(&mut self, filter: &Filter, line: &str) -> io::Result<()> {
        let Some(dir) = self.dir.clone() else {
            return Ok(());
        };

        self.reconcile(&filter.rings);

        for buffer in &mut self.buffers {
            if buffer.config.pattern != "*" && !line.contains(&buffer.config.pattern) {
                continue;
            }

            let started_at = Instant::now();

            let stored_line = bounded_ring_line(line, buffer.config.byte_budget);
            let stored_bytes = ring_line_bytes(&stored_line);

            while !buffer.lines.is_empty()
                && (buffer.lines.len() >= buffer.config.capacity
                    || buffer.bytes.saturating_add(stored_bytes) > buffer.config.byte_budget)
            {
                if let Some(evicted) = buffer.lines.pop_front() {
                    buffer.bytes = buffer.bytes.saturating_sub(ring_line_bytes(&evicted));
                    buffer.evictions += 1;
                    self.metrics.record(
                        &filter.metrics,
                        "edgelog_ring_evictions_total",
                        MetricDims {
                            ring: Some(buffer.config.name.clone()),
                            outcome: Some("evicted".to_string()),
                            ..MetricDims::default()
                        },
                    );
                }
            }

            buffer.bytes = buffer.bytes.saturating_add(stored_bytes);
            buffer.lines.push_back(stored_line);
            fs::create_dir_all(&dir)?;
            fs::write(
                dir.join(format!("{}.log", buffer.config.name)),
                buffer
                    .lines
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join("\n")
                    + "\n",
            )?;
            self.metrics.record(
                &filter.metrics,
                "edgelog_ring_writes_total",
                MetricDims {
                    ring: Some(buffer.config.name.clone()),
                    outcome: Some("written".to_string()),
                    ..MetricDims::default()
                },
            );
            self.traces.record(
                &filter.traces,
                "ring.write",
                started_at,
                vec![
                    ("ring", buffer.config.name.clone()),
                    ("outcome", "written".to_string()),
                ],
            );
        }

        Ok(())
    }

    fn reconcile(&mut self, configs: &[RingConfig]) {
        self.buffers.retain(|buffer| {
            configs
                .iter()
                .any(|config| config.name == buffer.config.name)
        });

        for config in configs {
            match self
                .buffers
                .iter_mut()
                .find(|buffer| buffer.config.name == config.name)
            {
                Some(buffer) => {
                    buffer.config = config.clone();
                    while buffer.lines.len() > config.capacity || buffer.bytes > config.byte_budget
                    {
                        if let Some(evicted) = buffer.lines.pop_front() {
                            buffer.bytes = buffer.bytes.saturating_sub(ring_line_bytes(&evicted));
                            buffer.evictions += 1;
                        } else {
                            break;
                        }
                    }
                }
                None => self.buffers.push(NamedRing {
                    config: config.clone(),
                    lines: VecDeque::new(),
                    bytes: 0,
                    evictions: 0,
                }),
            }
        }
    }
}

fn ring_line_bytes(line: &str) -> usize {
    line.len() + 1
}

fn bounded_ring_line(line: &str, byte_budget: usize) -> String {
    let max_line_bytes = byte_budget.saturating_sub(1);

    if line.len() <= max_line_bytes {
        return line.to_string();
    }

    let omitted = line.len().saturating_sub(max_line_bytes);
    let marker = format!(" ...[edgelog truncated {omitted} bytes]");
    let keep = max_line_bytes.saturating_sub(marker.len());
    let prefix = utf8_prefix(line, keep);
    let mut out = format!("{prefix}{marker}");

    while out.len() > max_line_bytes {
        out.pop();
    }

    out
}

fn utf8_prefix(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }

    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }

    &value[..end]
}

struct OutputControls {
    sampled: usize,
    throttle_window_started: Instant,
    emitted_this_window: u64,
}

enum OutputDecision {
    Emit,
    Sampled,
    Throttled,
}

impl Default for OutputControls {
    fn default() -> Self {
        Self {
            sampled: 0,
            throttle_window_started: Instant::now(),
            emitted_this_window: 0,
        }
    }
}

impl OutputControls {
    fn decide(&mut self, filter: &Filter) -> OutputDecision {
        self.sampled += 1;

        if self.sampled % filter.sample_every != 0 {
            return OutputDecision::Sampled;
        }

        let Some(limit) = filter.throttle_per_second else {
            return OutputDecision::Emit;
        };

        if self.throttle_window_started.elapsed() >= Duration::from_secs(1) {
            self.throttle_window_started = Instant::now();
            self.emitted_this_window = 0;
        }

        if self.emitted_this_window >= limit {
            return OutputDecision::Throttled;
        }

        self.emitted_this_window += 1;
        OutputDecision::Emit
    }
}

struct DownstreamHops {
    dir: Option<PathBuf>,
    active: Vec<HopConfig>,
    metrics: Arc<Metrics>,
    traces: Arc<Traces>,
}

impl DownstreamHops {
    fn new(dir: Option<PathBuf>, metrics: Arc<Metrics>, traces: Arc<Traces>) -> Self {
        Self {
            dir,
            active: Vec::new(),
            metrics,
            traces,
        }
    }

    fn observe(&mut self, filter: &Filter, line: &str) -> io::Result<()> {
        let Some(dir) = self.dir.clone() else {
            return Ok(());
        };

        self.reconcile(&filter.hops, &dir)?;

        for hop in &self.active {
            if line.contains(&hop.pattern) {
                let started_at = Instant::now();
                fs::create_dir_all(&dir)?;
                use std::io::Write;
                let mut file = fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(dir.join(format!("{}.log", hop.name)))?;
                writeln!(file, "{line}")?;
                self.metrics.record(
                    &filter.metrics,
                    "edgelog_hop_writes_total",
                    MetricDims {
                        hop: Some(hop.name.clone()),
                        outcome: Some("written".to_string()),
                        ..MetricDims::default()
                    },
                );
                self.traces.record(
                    &filter.traces,
                    "hop.write",
                    started_at,
                    vec![
                        ("hop", hop.name.clone()),
                        ("outcome", "written".to_string()),
                    ],
                );
            }
        }

        Ok(())
    }

    fn reconcile(&mut self, configs: &[HopConfig], dir: &Path) -> io::Result<()> {
        for old in &self.active {
            if !configs.iter().any(|new| new.name == old.name) {
                fs::create_dir_all(dir)?;
                fs::write(dir.join(format!("{}.removed", old.name)), "removed\n")?;
            }
        }

        self.active = configs.to_vec();
        Ok(())
    }
}

fn record_line_span(
    traces: &Traces,
    filter: &Filter,
    started_at: Instant,
    outcome: &str,
    line: &str,
) {
    let mut attrs = vec![("outcome", outcome.to_string())];

    if filter.traces.include_line {
        attrs.push(("line", line.to_string()));
    }

    traces.record(&filter.traces, "line.process", started_at, attrs);
}

fn emit_stdout_line(filter: &Filter, node_id: &str, line: &str) -> bool {
    if !filter.output.stdout_enabled {
        return false;
    }

    let mut output = String::new();

    if let Some(prefix) = &filter.output.stdout_prefix {
        output.push_str(prefix);
    }

    if filter.output.stdout_tag {
        output.push_str("node=");
        output.push_str(node_id);
        output.push_str(" mode=");
        output.push_str(match filter.mode {
            Mode::Include => "include",
            Mode::Exclude => "exclude",
        });
        output.push_str(" message=");
    }

    output.push_str(line);
    println!("{output}");
    true
}

fn record_stdout_disabled(metrics: &Metrics, traces: &Traces, filter: &Filter, started_at: Instant, line: &str) {
    metrics.record(
        &filter.metrics,
        "edgelog_output_drops_total",
        MetricDims {
            outcome: Some("stdout_disabled".to_string()),
            ..MetricDims::default()
        },
    );
    record_line_span(traces, filter, started_at, "stdout_disabled", line);
}

fn filter_stdin(
    filter: &mut LiveFilter,
    rings: &mut RingBuffers,
    hops: &mut DownstreamHops,
    controls: &mut OutputControls,
    metrics: &Metrics,
    traces: &Traces,
) -> io::Result<()> {
    let stdin = io::stdin();

    for line in stdin.lock().lines() {
        let started_at = Instant::now();
        let line = line?;
        let snapshot = filter.snapshot();
        metrics.record(
            &snapshot.metrics,
            "edgelog_input_lines_total",
            MetricDims::default(),
        );
        rings.observe(&snapshot, &line)?;
        hops.observe(&snapshot, &line)?;

        if !snapshot.allows(&line) {
            metrics.record(
                &snapshot.metrics,
                "edgelog_output_drops_total",
                MetricDims {
                    outcome: Some("filtered".to_string()),
                    ..MetricDims::default()
                },
            );
            record_line_span(traces, &snapshot, started_at, "filtered", &line);
            continue;
        }

        match controls.decide(&snapshot) {
            OutputDecision::Emit => {
                if emit_stdout_line(&snapshot, &metrics.node_id, &line) {
                    metrics.record(
                        &snapshot.metrics,
                        "edgelog_stdout_lines_total",
                        MetricDims {
                            outcome: Some("emitted".to_string()),
                            ..MetricDims::default()
                        },
                    );
                    record_line_span(traces, &snapshot, started_at, "emitted", &line);
                } else {
                    record_stdout_disabled(metrics, traces, &snapshot, started_at, &line);
                }
            }
            OutputDecision::Sampled => {
                metrics.record(
                    &snapshot.metrics,
                    "edgelog_output_drops_total",
                    MetricDims {
                        outcome: Some("sampled".to_string()),
                        ..MetricDims::default()
                    },
                );
                record_line_span(traces, &snapshot, started_at, "sampled", &line);
            }
            OutputDecision::Throttled => {
                metrics.record(
                    &snapshot.metrics,
                    "edgelog_output_drops_total",
                    MetricDims {
                        outcome: Some("throttled".to_string()),
                        ..MetricDims::default()
                    },
                );
                record_line_span(traces, &snapshot, started_at, "throttled", &line);
            }
        }
    }

    Ok(())
}

fn tail_file(
    path: &Path,
    from_end: bool,
    filter: &mut LiveFilter,
    rings: &mut RingBuffers,
    hops: &mut DownstreamHops,
    controls: &mut OutputControls,
    metrics: &Metrics,
    traces: &Traces,
) -> io::Result<()> {
    let mut position = 0;

    loop {
        match File::open(path) {
            Ok(mut file) => {
                let len = file.metadata()?.len();

                if from_end && position == 0 {
                    position = len;
                } else if position > len {
                    position = 0;
                }

                file.seek(SeekFrom::Start(position))?;
                let mut reader = BufReader::new(file);
                let mut line = String::new();

                loop {
                    line.clear();
                    let bytes = reader.read_line(&mut line)?;

                    if bytes == 0 {
                        break;
                    }

                    position += bytes as u64;

                    if line.ends_with('\n') {
                        line.pop();
                        if line.ends_with('\r') {
                            line.pop();
                        }
                    }

                    let started_at = Instant::now();
                    let snapshot = filter.snapshot();
                    metrics.record(
                        &snapshot.metrics,
                        "edgelog_input_lines_total",
                        MetricDims::default(),
                    );
                    rings.observe(&snapshot, &line)?;
                    hops.observe(&snapshot, &line)?;

                    if !snapshot.allows(&line) {
                        metrics.record(
                            &snapshot.metrics,
                            "edgelog_output_drops_total",
                            MetricDims {
                                outcome: Some("filtered".to_string()),
                                ..MetricDims::default()
                            },
                        );
                        record_line_span(traces, &snapshot, started_at, "filtered", &line);
                        continue;
                    }

                    match controls.decide(&snapshot) {
                        OutputDecision::Emit => {
                            if emit_stdout_line(&snapshot, &metrics.node_id, &line) {
                                metrics.record(
                                    &snapshot.metrics,
                                    "edgelog_stdout_lines_total",
                                    MetricDims {
                                        outcome: Some("emitted".to_string()),
                                        ..MetricDims::default()
                                    },
                                );
                                record_line_span(traces, &snapshot, started_at, "emitted", &line);
                            } else {
                                record_stdout_disabled(
                                    metrics,
                                    traces,
                                    &snapshot,
                                    started_at,
                                    &line,
                                );
                            }
                        }
                        OutputDecision::Sampled => {
                            metrics.record(
                                &snapshot.metrics,
                                "edgelog_output_drops_total",
                                MetricDims {
                                    outcome: Some("sampled".to_string()),
                                    ..MetricDims::default()
                                },
                            );
                            record_line_span(traces, &snapshot, started_at, "sampled", &line);
                        }
                        OutputDecision::Throttled => {
                            metrics.record(
                                &snapshot.metrics,
                                "edgelog_output_drops_total",
                                MetricDims {
                                    outcome: Some("throttled".to_string()),
                                    ..MetricDims::default()
                                },
                            );
                            record_line_span(traces, &snapshot, started_at, "throttled", &line);
                        }
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                eprintln!("edgelog: waiting for {}", path.display());
            }
            Err(error) => return Err(error),
        }

        thread::sleep(POLL_INTERVAL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tunnel_accepts_optional_protocol_kind() {
        let tunnel = parse_tunnel("admin 127.0.0.1:8080 http").unwrap();
        assert_eq!(tunnel.name, "admin");
        assert_eq!(tunnel.addr, "127.0.0.1:8080");
        assert_eq!(tunnel.kind, TunnelKind::Http);

        let tunnel = parse_tunnel("node-debug 127.0.0.1:9229 debugger").unwrap();
        assert_eq!(tunnel.kind, TunnelKind::Debugger);

        let tunnel = parse_tunnel("raw 127.0.0.1:9000").unwrap();
        assert_eq!(tunnel.kind, TunnelKind::Tcp);
    }

    #[test]
    fn parse_tunnel_rejects_unsafe_names_and_unknown_kinds() {
        assert!(parse_tunnel("../admin 127.0.0.1:8080 http").is_none());
        assert!(parse_tunnel("admin 127.0.0.1:8080 ftp").is_none());
        assert!(parse_tunnel("admin 127.0.0.1 tcp").is_none());
    }

    #[test]
    fn stream_routing_detects_connect_under_multiple_routes() {
        assert!(control_command_is_stream("CONNECT admin"));
        assert!(control_command_is_stream(
            "ROUTE region ROUTE cluster ROUTE pod CONNECT admin"
        ));
        assert!(!control_command_is_stream("ROUTE leaf FOLLOW errors 20"));
        assert!(!control_command_is_stream("TAIL errors 20"));
    }
}
