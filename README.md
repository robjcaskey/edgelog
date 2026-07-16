# edgelog

`edgelog` is a small Kubernetes sidecar that tails an application log file, filters
lines, and writes the filtered stream to stdout. Kubernetes can then collect the
sidecar's stdout with the normal node log collector.

The filter config is reloaded live when the file changes, so updating a mounted
ConfigMap changes filtering without restarting the pod.

For direct live editing, mount a writable file path and run with
`--create-config`. If the file does not exist, `edgelog` writes a starter config
and then reloads it whenever you edit it.

## Filter config

`/etc/edgelog/filter.conf`:

```text
# mode=include keeps only matching lines.
# mode=exclude drops matching lines.
mode=exclude

healthcheck
debug=true
GET /metrics
```

Rules are literal substring matches. Empty lines and `#` comments are ignored.
If there are no patterns, all lines pass.

Optional controls:

```text
sample=10
throttle_per_second=50
clear_patterns
clear_throttle
ring recent-errors 100 ERROR
peer leaf-a 10.0.0.12:7777
upstream 10.0.0.1:7777
tunnel admin-http 127.0.0.1:8080 http
tunnel debug-session 127.0.0.1:9229 debugger
metrics=on
metric_label node=on
metric_label ring=off
statsd 127.0.0.1:8125
statsd_prefix=edgelog
traces=on
trace_file /var/run/edgelog/spans.jsonl
trace_sample=100
trace_line=off
```

`sample=10` emits every tenth allowed line to stdout.
`throttle_per_second=50` emits at most 50 allowed lines per second to stdout.
`clear_patterns` removes all earlier filter patterns.
`clear_throttle` removes any earlier stdout throttle.
`ring NAME CAPACITY PATTERN` writes an on-disk ring buffer snapshot for matching
lines when `--buffers-dir` is set. Use `*` as the pattern to capture every line.
`peer NAME HOST:PORT` defines a named TCP control hop that can be routed through.
`upstream HOST:PORT` registers this node with a parent control server, usually
the mothership or the next hop toward it.
`tunnel NAME HOST:PORT [tcp|http|websocket|debugger]` enables a named TCP tunnel
target while that config line exists. HTTP, WebSocket, debugger, and raw TCP
protocols all ride the same byte stream; the kind is descriptive for operators.
`metrics=on` enables Prometheus and StatsD-compatible counters.
`metric_label LABEL=on|off` controls Prometheus labels and StatsD tags for
`node`, `ring`, `hop`, `command`, and `outcome`.
`statsd HOST:PORT` emits Datadog-style tagged StatsD counters. Use `statsd off`
to disable StatsD emission.
`statsd_prefix=NAME` changes the StatsD metric prefix.
`traces=on` enables lightweight JSONL spans for line processing, ring writes,
hop writes, and control requests.
`trace_file PATH` appends spans to a file. Use `trace_file off` to disable the
file sink.
`trace_tcp HOST:PORT` writes each span as one JSON line to a TCP sink. Use
`trace_tcp off` to disable it.
`trace_sample=N` emits every Nth span.
`trace_line=on|off` controls whether raw log lines are included in line spans.

Temporary rules:

```text
temp debug-boost 5m clear_patterns
temp debug-boost 5m clear_throttle
temp debug-boost 5m sample=1
```

`temp NAME DURATION DIRECTIVE` applies the directive once for that duration after
the config reload that introduced it. Supported durations are seconds, minutes,
and hours, for example `30s`, `5m`, and `1h`. Leaving the same temporary rule in
the file does not re-trigger it after it expires; edit or rename it to start a
new one-off window.

## Usage

```bash
edgelog --config /etc/edgelog/filter.conf --input /var/log/app/app.log --from-end
```

Writable live-edit config:

```bash
edgelog --config /var/run/edgelog/filter.conf --create-config --input /var/log/app/app.log
```

Environment variables are also supported:

```text
EDGLOG_CONFIG=/etc/edgelog/filter.conf
EDGLOG_INPUT=/var/log/app/app.log
EDGLOG_FROM_END=1
EDGLOG_CREATE_CONFIG=1
EDGLOG_BUFFERS_DIR=/var/run/edgelog/buffers
EDGLOG_HOPS_DIR=/var/run/edgelog/hops
EDGLOG_CONTROL_LISTEN=0.0.0.0:7777
EDGLOG_NODE_ID=leaf-a
EDGLOG_REGISTER_ADDR=leaf-a.default.svc.cluster.local:7777
EDGLOG_CONTROL_ONLY=1
EDGLOG_PROMETHEUS_LISTEN=0.0.0.0:9100
```

Without `--input`, `edgelog` filters stdin.

## User story: tail pod logs from the CLI

The basic flow is:

1. The application writes logs to a shared file, for example
   `/var/log/app/app.log`.
2. The `edgelog` sidecar tails that file from the same pod volume.
3. The live config defines local ring buffers for the streams users care about.
4. Each sidecar exposes a TCP control port and registers upward toward the
   mothership.
5. The user runs `edgelog-tail` from their workstation or jumpbox and tails the
   remote ring through the mothership.

In the pod, mount one writable volume for app logs and one for edgelog state:

```yaml
volumes:
  - name: app-logs
    emptyDir: {}
  - name: edgelog-state
    emptyDir: {}
```

The app writes its normal logs into the shared log volume:

```yaml
containers:
  - name: app
    image: example/app:latest
    volumeMounts:
      - name: app-logs
        mountPath: /var/log/app
    env:
      - name: LOG_FILE
        value: /var/log/app/app.log
```

The sidecar tails that file, keeps local rings, and connects back up:

```yaml
  - name: edgelog
    image: example/edgelog:latest
    args:
      - --config
      - /var/run/edgelog/filter.conf
      - --create-config
      - --input
      - /var/log/app/app.log
      - --buffers-dir
      - /var/run/edgelog/buffers
      - --control-listen
      - 0.0.0.0:7777
      - --node-id
      - checkout-api-a
      - --register-addr
      - checkout-api-a.default.svc.cluster.local:7777
    volumeMounts:
      - name: app-logs
        mountPath: /var/log/app
      - name: edgelog-state
        mountPath: /var/run/edgelog
```

The live config defines the rings users can tail:

```text
mode=include
ring errors 200 ERROR
ring requests 500 request_id=
ring all 1000 *
upstream edgelog-mothership.default.svc.cluster.local:7000
```

From the user side, tail a local node directly:

```bash
edgelog-tail --server checkout-api-a.default.svc.cluster.local:7777 \
  --ring errors \
  --lines 50
```

Or tail through the mothership:

```bash
edgelog-tail --server edgelog-mothership.default.svc.cluster.local:7000 \
  --path checkout-api-a \
  --ring errors \
  --lines 50
```

For multi-hop environments, use a slash-separated path:

```bash
edgelog-tail --server edgelog-mothership.default.svc.cluster.local:7000 \
  --path region-east/cluster-prod/namespace-default/checkout-api-a \
  --ring requests \
  --lines 100
```

By default `edgelog-tail` follows the stream, so the user experience is close to:

```bash
tail -f /var/run/edgelog/buffers/errors.log
```

but the file stays local to the pod, and the CLI reaches it through the TCP hop
chain.

When the user needs more detail for a short investigation, edit the live config:

```text
temp debug-boost 5m clear_patterns
temp debug-boost 5m sample=1
temp ring-debug 5m metric_label ring=on
```

After five minutes, the sidecar falls back to the base filter and low-cardinality
metrics automatically.

## Kubernetes example

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: edgelog-filter
data:
  filter.conf: |
    mode=exclude
    healthcheck
    GET /metrics
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: example-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: example-app
  template:
    metadata:
      labels:
        app: example-app
    spec:
      volumes:
        - name: app-logs
          emptyDir: {}
        - name: edgelog-filter
          configMap:
            name: edgelog-filter
      containers:
        - name: app
          image: example/app:latest
          volumeMounts:
            - name: app-logs
              mountPath: /var/log/app
          env:
            - name: LOG_FILE
              value: /var/log/app/app.log
        - name: edgelog
          image: example/edgelog:latest
          args:
            - --config
            - /etc/edgelog/filter.conf
            - --input
            - /var/log/app/app.log
            - --from-end
          volumeMounts:
            - name: app-logs
              mountPath: /var/log/app
            - name: edgelog-filter
              mountPath: /etc/edgelog
              readOnly: true
```

## Kubernetes example with a live-editable config file

This uses a writable `emptyDir` for the filter config. You can edit the live file
with `kubectl exec` or by mounting the same volume into another helper sidecar.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: example-app-live-edit
spec:
  replicas: 1
  selector:
    matchLabels:
      app: example-app-live-edit
  template:
    metadata:
      labels:
        app: example-app-live-edit
    spec:
      volumes:
        - name: app-logs
          emptyDir: {}
        - name: edgelog-config
          emptyDir: {}
      containers:
        - name: app
          image: example/app:latest
          volumeMounts:
            - name: app-logs
              mountPath: /var/log/app
          env:
            - name: LOG_FILE
              value: /var/log/app/app.log
        - name: edgelog
          image: example/edgelog:latest
          args:
            - --config
            - /var/run/edgelog/filter.conf
            - --create-config
            - --input
            - /var/log/app/app.log
            - --from-end
          volumeMounts:
            - name: app-logs
              mountPath: /var/log/app
            - name: edgelog-config
              mountPath: /var/run/edgelog
```

Edit it live:

```bash
kubectl exec deploy/example-app-live-edit -c edgelog -- sh -c \
  "printf 'mode=exclude\nhealthcheck\nGET /metrics\n' > /var/run/edgelog/filter.conf"
```

## Local live-edit demo

Start the spammer and write its output to a file:

```bash
mkdir -p /tmp/edgelog-demo
cargo run --example spammer > /tmp/edgelog-demo/app.log
```

In another terminal, start `edgelog`:

```bash
cargo run --bin edgelog -- --config /tmp/edgelog-demo/filter.conf \
  --create-config \
  --input /tmp/edgelog-demo/app.log
```

Show only `lovely spam`:

```bash
printf 'mode=include\nlovely spam\n' > /tmp/edgelog-demo/filter.conf
```

Show only `wonderful spam`:

```bash
printf 'mode=include\nwonderful spam\n' > /tmp/edgelog-demo/filter.conf
```

Show all spam again:

```bash
printf 'mode=include\n' > /tmp/edgelog-demo/filter.conf
```

Throttle stdout to one matching line per second:

```bash
printf 'mode=include\nlovely spam\nthrottle_per_second=1\n' > /tmp/edgelog-demo/filter.conf
```

Sample stdout to every third matching line:

```bash
printf 'mode=include\nlovely spam\nsample=3\n' > /tmp/edgelog-demo/filter.conf
```

Let all lines through for five minutes, then automatically return to the base
config:

```bash
printf 'mode=include\nlovely spam\ntemp debug-boost 5m clear_patterns\ntemp debug-boost 5m clear_throttle\ntemp debug-boost 5m sample=1\n' \
  > /tmp/edgelog-demo/filter.conf
```

## On-disk ring buffers

Start `edgelog` with a writable ring-buffer directory:

```bash
cargo run --bin edgelog -- --config /tmp/edgelog-demo/filter.conf \
  --create-config \
  --input /tmp/edgelog-demo/app.log \
  --buffers-dir /tmp/edgelog-demo/buffers
```

Define ring buffers in the live config:

```bash
printf 'mode=include\nring lovely 5 lovely spam\nring wonderful 5 wonderful spam\n' \
  > /tmp/edgelog-demo/filter.conf
```

Read the on-disk buffers:

```bash
cat /tmp/edgelog-demo/buffers/lovely.log
cat /tmp/edgelog-demo/buffers/wonderful.log
```

Changing capacity or removing a `ring` line takes effect live.

## Downstream hops

Downstream hops are live-configured append-only files for other containers,
agents, or log processors that mount the same volume.

Start with a writable hops directory:

```bash
cargo run --bin edgelog -- --config /tmp/edgelog-demo/filter.conf \
  --create-config \
  --input /tmp/edgelog-demo/app.log \
  --hops-dir /tmp/edgelog-demo/hops
```

Add hops live:

```bash
printf 'mode=include\nhop lovely lovely spam\nhop wonderful wonderful spam\n' \
  > /tmp/edgelog-demo/filter.conf
```

Read downstream hop streams:

```bash
tail -f /tmp/edgelog-demo/hops/lovely.log
tail -f /tmp/edgelog-demo/hops/wonderful.log
```

Remove a hop by deleting its `hop` line from the config. `edgelog` stops writing
that hop immediately and writes `NAME.removed` as a simple on-disk tombstone for
downstream consumers that need to notice removal.

## TCP control plane

Each `edgelog` process can expose a tiny line-based TCP server. It can serve
local ring buffers, route commands through named peers, and accept registrations
from downstream nodes.

Start a leaf with a control server and local ring buffers:

```bash
cargo run --bin edgelog -- --config /tmp/edgelog-demo/filter.conf \
  --create-config \
  --input /tmp/edgelog-demo/app.log \
  --buffers-dir /tmp/edgelog-demo/buffers \
  --control-listen 127.0.0.1:7777 \
  --node-id leaf-a \
  --register-addr 127.0.0.1:7777
```

Start a mothership server with no local log input:

```bash
cargo run --bin edgelog -- --config /tmp/edgelog-mothership/filter.conf \
  --create-config \
  --control-listen 127.0.0.1:7000 \
  --node-id mothership \
  --control-only
```

Register a leaf with the mothership by adding an upstream to the leaf config:

```bash
printf 'mode=include\nring lovely 5 lovely spam\nupstream 127.0.0.1:7000\n' \
  > /tmp/edgelog-demo/filter.conf
```

Or define a static routed peer directly in the mothership config:

```bash
printf 'peer leaf-a 127.0.0.1:7777\n' > /tmp/edgelog-mothership/filter.conf
```

Use the custom tail client against a local node:

```bash
cargo run --bin edgelog-tail -- --server 127.0.0.1:7777 --ring lovely --lines 20
```

Drill down from the mothership through a named hop and tail the remote ring:

```bash
cargo run --bin edgelog-tail -- --server 127.0.0.1:7000 \
  --path leaf-a \
  --ring lovely \
  --lines 20
```

List known peers or rings:

```bash
cargo run --bin edgelog-tail -- --server 127.0.0.1:7000 --peers --no-follow
cargo run --bin edgelog-tail -- --server 127.0.0.1:7777 --rings --no-follow
```

The TCP protocol is intentionally plain text:

```text
PING
REGISTER leaf-a 127.0.0.1:7777
PEERS
RINGS
TUNNELS
TAIL lovely 20
FOLLOW lovely 20
CONNECT admin-http
ROUTE leaf-a FOLLOW lovely 20
ROUTE leaf-a CONNECT admin-http
```

## Live port tunnels

Named tunnels are live-controlled by the same config file. Adding a `tunnel`
line allows new connections; removing or changing the line closes active
connections for that tunnel and rejects new ones.

Expose an internal-only HTTP endpoint and a debugger socket from a leaf:

```text
tunnel admin-http 127.0.0.1:8080 http
tunnel node-debug 127.0.0.1:9229 debugger
```

Forward the HTTP endpoint through the mothership and one leaf hop:

```bash
cargo run --bin edgelog-connect -- \
  --server 127.0.0.1:7000 \
  --path leaf-a \
  --target admin-http \
  --local-listen 127.0.0.1:18080
```

Now local HTTP and WebSocket clients can use the forwarded port:

```bash
curl http://127.0.0.1:18080/healthz
```

Forward a live debugger session the same way:

```bash
cargo run --bin edgelog-connect -- \
  --server 127.0.0.1:7000 \
  --path leaf-a \
  --target node-debug \
  --local-listen 127.0.0.1:19229
```

Multiple layers use the existing slash-separated route path:

```bash
cargo run --bin edgelog-connect -- \
  --server 127.0.0.1:7000 \
  --path region-east/cluster-prod/namespace-default/checkout-api-a \
  --target admin-http \
  --local-listen 127.0.0.1:18080
```

List currently enabled tunnel targets:

```bash
cargo run --bin edgelog-connect -- \
  --server 127.0.0.1:7000 \
  --path leaf-a \
  --tunnels
```

## Console audit footprint

At the edge, `edgelog` should behave like the world's most welcome house guest:
small footprint, no surprise writes, and clear accounting when memory is reused.

By default, tunnel and debugger session audit state must be in-memory only. It
must not create disk files, local databases, or hidden durable logs unless an
operator explicitly configures a destination.

The default audit budget is fixed at 32 KiB total:

```text
total audit memory: 32 KiB
master audit ring:  1 KiB
payload rings:      31 KiB shared across active/recent sessions
```

The master ring is reserved for compact facts:

```text
session opened
session closed
who/peer identity when known
target name and kind
login/connect time
logout/disconnect time
bytes in and out
payload ring overflow
session summary eviction
audit chain head
```

Input and output visibility should use tiny per-session byte rings. Those rings
store bounded previews of traffic direction and timing, not an unbounded copy of
the stream. HTTP, WebSocket, debugger, and raw TCP tunnels all follow the same
rule.

Overflow is expected and must be visible. When any audit ring evicts old data,
`edgelog` records a compact overflow event in the 1 KiB master ring and
increments audit metrics. If the master ring itself wraps, the chain head,
wrap/eviction counters, and latest master entries still show that older master
items were evicted.

Audit entries should be tamper-evident with a process-local hash chain:

```text
entry_hash = sha256(previous_hash || sequence || timestamp || event)
```

This is not absolute tamper-proof storage. A host-level attacker that can rewrite
process memory or all configured destinations can still lie. The goal is a tiny,
bounded, default-on-memory record that makes normal session history, overflow,
and accidental deletion visible without leaving files behind.

Durable or remote audit destinations are opt-in only. When configured later, they
should receive the same compact audit events and bounded input/output previews;
they should not change the edge default.

## Logging output policy

Most deployments need to decide what happens to each accepted line:

```text
stdout=on|off
stdout_tag=on|off
stdout_prefix=TEXT
buffer_stdout=none|line|ring
ring NAME CAPACITY PATTERN
hop NAME PATTERN
```

The default should stay Kubernetes-friendly: accepted lines go to stdout, and the
node's normal log collector can pick them up. Turning stdout off should be
explicit and visible in metrics, because it means `edgelog` is no longer
contributing to the pod's normal log stream.

Tags should be opt-in for stdout. Plain application logs are often parsed by
existing collectors, so `edgelog` should not rewrite them by default. When tags
are enabled, they should be compact and stable, for example:

```text
node=checkout-api-a tunnel=admin-http direction=out message=...
```

Buffering should be bounded and explicit:

```text
none  - write accepted lines directly to stdout
line  - keep only enough memory to finish the current line
ring  - keep a named, capacity-bounded ring for replay or local inspection
```

No logging mode should create an unbounded queue. If stdout or a downstream sink
falls behind, `edgelog` should either block the producer path intentionally or
drop from a bounded ring and record the drop/overflow in metrics and the master
audit ring. Silent growth is not allowed.

The important dimensions to make configurable are:

```text
destination: stdout, named ring, hop file, control tunnel, future remote sink
line shape: raw, tagged, prefixed, redacted
buffering: direct, one-line, bounded ring
overflow: block, drop-oldest, drop-newest
accounting: counters plus compact audit master entries
```

Payload previews for console/tunnel audit follow the same rule: tiny bounded
rings, clear overflow accounting, and no default disk persistence.

## Adoption stories

### Preferred: app writes to a shared log file

In this mode, the main application does not write its operational log stream to
stdout when it is connected to `edgelog`. It writes to a shared file instead, and
the `edgelog` sidecar tails that file.

This is usually the best fit for Kubernetes:

```text
app -> /var/log/app/app.log
edgelog sidecar -> filtered stdout, rings, hops, tunnels, metrics
node log collector -> edgelog stdout
```

Benefits:

```text
the app does not need to know about Kubernetes log collection
edgelog can decide what reaches stdout
edgelog can keep tiny bounded rings for local replay
stdout can stay quiet during high-volume or debug-heavy periods
filter/tag/buffer policy can change live without restarting the app
```

Container shape:

```yaml
volumes:
  - name: app-logs
    emptyDir: {}
  - name: edgelog-state
    emptyDir: {}

containers:
  - name: app
    image: example/app:latest
    env:
      - name: LOG_FILE
        value: /var/log/app/app.log
      - name: LOG_TO_STDOUT
        value: "false"
    volumeMounts:
      - name: app-logs
        mountPath: /var/log/app

  - name: edgelog
    image: example/edgelog:latest
    args:
      - --config
      - /var/run/edgelog/filter.conf
      - --create-config
      - --input
      - /var/log/app/app.log
      - --from-end
      - --buffers-dir
      - /var/run/edgelog/buffers
      - --control-listen
      - 0.0.0.0:7777
    volumeMounts:
      - name: app-logs
        mountPath: /var/log/app
      - name: edgelog-state
        mountPath: /var/run/edgelog
```

The application can still expose its normal HTTP, WebSocket, admin, or debugger
ports on loopback or pod-local interfaces. `edgelog` can conditionally expose
those with `tunnel` lines when operators need them.

### Compatibility: app stdout is piped into edgelog

Some applications only know how to log to stdout. For those, run the app under a
small wrapper and pipe stdout into `edgelog`:

```text
app stdout -> edgelog stdin -> filtered stdout, rings, hops, tunnels, metrics
```

Example wrapper:

```bash
set -euo pipefail

/app/server 2>&1 | edgelog \
  --config /var/run/edgelog/filter.conf \
  --create-config \
  --buffers-dir /var/run/edgelog/buffers \
  --control-listen 0.0.0.0:7777
```

This is easier to adopt, but it is less clean:

```text
the app and edgelog share one process pipeline
if the pipe blocks, the app's stdout path can block
restart and signal behavior need wrapper care
the original app stdout is no longer directly visible
```

Use this mode when changing the app's logging destination is not practical. When
the app can write to a file or socket directly, prefer the shared-file sidecar
mode.

## Spam trio demo

The repository includes a deliberately noisy sample app for trying these modes
under pressure:

```text
api-gateway  - fake GraphQL gateway; preferred shared-file logging
job-worker   - fake background worker; preferred shared-file logging
db-server    - fake legacy database; bolt-on stdout pipe logging
```

Run one service directly:

```bash
cargo run --example spam_trio -- \
  --role api-gateway \
  --log-file /tmp/edgelog-spam/api.log \
  --duration-seconds 10 \
  --burst 1000 \
  --tick-ms 50 \
  --payload-bytes 32
```

The burst controls are intentionally blunt:

```text
--burst N          lines emitted per tick
--tick-ms N        milliseconds between bursts
--payload-bytes N  synthetic payload width per line
```

For example, `--burst 1000 --tick-ms 50` emits about 20,000 lines per second for
one service.

Run the full trio through `edgelog`:

```bash
examples/run_spam_trio_demo.sh
```

The demo creates a temporary root at `/tmp/edgelog-spam-trio`, starts two
shared-file services, pipes the legacy DB service through `edgelog`, and prints a
small summary of source logs, filtered stdout captures, and ring-buffer sizes.

Useful knobs:

```bash
SPAM_DURATION=5 \
SPAM_BURST=1000 \
SPAM_TICK_MS=50 \
SPAM_PAYLOAD_BYTES=32 \
examples/run_spam_trio_demo.sh
```

This is meant to make overflow and filtering behavior obvious. The source logs
can be very large, while filtered stdout and named rings should remain bounded by
their config.

## Metrics

Metrics are controlled by the same live-reloaded config file:

```text
metrics=on
metric_label node=on
metric_label outcome=on
metric_label ring=off
metric_label hop=off
metric_label command=off
statsd 127.0.0.1:8125
statsd_prefix=edgelog
```

Start a Prometheus-compatible endpoint:

```bash
cargo run --bin edgelog -- --config /tmp/edgelog-demo/filter.conf \
  --input /tmp/edgelog-demo/app.log \
  --buffers-dir /tmp/edgelog-demo/buffers \
  --prometheus-listen 127.0.0.1:9100
```

Scrape it:

```bash
curl http://127.0.0.1:9100/metrics
```

Available counters include:

```text
edgelog_input_lines_total
edgelog_stdout_lines_total
edgelog_output_drops_total
edgelog_ring_writes_total
edgelog_hop_writes_total
edgelog_control_requests_total
edgelog_tunnel_connects_total
```

Keep low cardinality by default, then temporarily turn on a high-cardinality
label when you need it:

```bash
printf 'metrics=on\nmetric_label node=on\nmetric_label outcome=on\nmetric_label ring=off\nring lovely 5 lovely spam\ntemp ring-debug 5m metric_label ring=on\n' \
  > /tmp/edgelog-demo/filter.conf
```

The same label policy is used for StatsD tags. With `metric_label ring=on`,
StatsD lines use Datadog-style tags like:

```text
edgelog.edgelog_ring_writes_total:1|c|#node:leaf-a,ring:lovely,outcome:written
```

## Traces

Traces are lightweight JSONL spans controlled by the live config:

```text
traces=on
trace_file /tmp/edgelog-demo/spans.jsonl
trace_sample=10
trace_line=off
```

Trace spans currently cover:

```text
line.process
ring.write
hop.write
control.request
tunnel.connect
```

Each span includes `trace_id`, `span_id`, `node`, `name`, `start_unix_nanos`,
`duration_us`, and an `attrs` object. Raw log lines are excluded unless
`trace_line=on`.

Temporarily capture every span and include raw log lines for five minutes:

```bash
printf 'traces=on\ntrace_file /tmp/edgelog-demo/spans.jsonl\ntrace_sample=100\ntrace_line=off\ntemp trace-debug 5m trace_sample=1\ntemp trace-debug 5m trace_line=on\n' \
  > /tmp/edgelog-demo/filter.conf
```
