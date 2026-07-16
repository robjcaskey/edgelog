# edgelog

> **Experimental, but likely useful.** `edgelog` is a working exploration of
> what logging and operational control can look like when they live close to a
> service. It is not yet hardened for production or untrusted networks.

Observability should help when a system is under stress, not become another
source of stress.

`edgelog` is a small edge companion for application logs. It can sit beside a
service, keep the bits of recent history an operator cares about, change what is
visible without restarting the application, and provide a narrow path back to
that context from elsewhere.

The point is not to replace a log platform. The point is to make the last few
feet between an application and its operators more adaptable.

## Why this exists

Centralized logging is valuable, but it is necessarily broad. During an
incident, the useful question is often much more local:

- Can I turn down a noisy health check right now?
- Can I briefly turn up one class of debug output without redeploying?
- Can I retain the latest matching events near the process that produced them?
- Can I reach a pod-local admin or debugger port through an explicit,
  short-lived control path?
- Can all of that stay small enough that the diagnostic system does not become
  the outage?

`edgelog` explores those questions with a deliberately modest toolkit: a text
file that reloads live, bounded rings, sampling and throttling, a tiny control
protocol, routable peers, opt-in tunnels, and optional metrics and traces.

## The sensibility

### Stay close to the source

Some decisions are best made at the edge, while the log line still has local
context and before it becomes part of a much larger stream. `edgelog` is meant
to run beside the application, commonly as a Kubernetes sidecar, and cooperate
with the logging system already in place.

By default, accepted lines remain ordinary stdout. Existing collectors can keep
doing what they already do. Tags, alternate destinations, metrics, traces, and
control features are choices rather than prerequisites.

### Prefer live and reversible controls

Operational curiosity should not require an application restart. The config is
an intentionally plain text file that is watched for changes. Filters,
sampling, throttles, rings, peers, tunnels, and telemetry can be attached or
removed while the process keeps running.

Temporary rules capture an especially important idea: heightened visibility
should be able to expire by itself. A five-minute debugging window is safer and
more honest than a forgotten permanent debug mode.

### Bound everything that can grow

Unbounded observability is a resource leak with better branding.

Rings have line and byte limits. Oversized lines are truncated deliberately.
The in-memory tunnel audit has a fixed 32 KiB budget, including a small master
history and bounded payload previews. Sampling and throttling make output
pressure explicit. Evictions and drops are counted rather than hidden.

The aim is not losslessness at any cost. The aim is predictable behavior under
pressure, with visible evidence when older detail has been sacrificed.

### Do not persist by surprise

Tunnel and debugger audit history is in memory by default. It does not quietly
create a database or a durable copy of interactive traffic. When configured,
log rings and downstream hops do write to operator-selected directories;
metrics and traces are likewise opt-in.

Persistence should be a decision, not a side effect.

### Keep the mechanism inspectable

The control plane is line-oriented TCP. The configuration is readable text.
Routing is a sequence of named peers. A tunnel exists because a corresponding
config line exists, and removing that line detaches it.

This is intentionally less magical than a large agent framework. Small tools
are easier to reason about at 3 a.m., easier to compose with shell and
Kubernetes primitives, and easier to discard if the experiment proves wrong.

### Account for the compromises

The in-memory audit history uses a process-local hash chain so accidental
rewrites and ordinary gaps are easier to notice. That does not make it
tamper-proof: a host-level attacker can still alter process memory or every
configured destination.

Likewise, a bounded ring necessarily forgets. `edgelog` treats overflow as a
normal operating condition that must be observable, not as an exceptional case
to conceal.

## A mental model

```text
application log file or stdin
            |
            v
         edgelog  <---- live text config
       /    |    \
      v     v     v
  stdout  bounded  selected local/remote paths
          rings    plus opt-in telemetry
```

At a larger scale, edge instances can register with an upstream node. The CLI
tools can route through those named peers to inspect a ring or connect to an
explicitly configured tunnel. The resulting shape is closer to a small nervous
system than a centralized archive: local reflexes, bounded local memory, and a
path for deliberate attention.

## What is here today

The repository builds three Rust binaries:

- `edgelog` tails a file or reads stdin, applies live policy, maintains rings,
  and optionally exposes control, metrics, tracing, and tunnels.
- `edgelog-tail` lists or follows named rings, including through routed peers.
- `edgelog-connect` reaches configured tunnel targets directly or through a
  peer path.

The implementation also includes temporary policy windows, Prometheus-style
counters, tagged StatsD output, lightweight JSONL/TCP traces, bounded audit
previews, and demos for noisy multi-service workloads.

## Try the smallest useful piece

Build and test:

```bash
cargo test
```

Create a live config:

```text
mode=exclude
healthcheck
GET /metrics

# Keep recent errors when --buffers-dir is configured.
ring recent-errors 100 ERROR

# Let all lines through for five minutes, once.
temp debug-window 5m clear_patterns
```

Then filter stdin:

```bash
cargo run --bin edgelog -- \
  --config /tmp/edgelog.conf \
  --buffers-dir /tmp/edgelog-rings
```

Or tail an application-owned file:

```bash
cargo run --bin edgelog -- \
  --config /tmp/edgelog.conf \
  --input /var/log/my-app/app.log \
  --from-end \
  --buffers-dir /tmp/edgelog-rings
```

Editing `/tmp/edgelog.conf` changes the active policy without restarting
`edgelog`.

## Experimental means experimental

The current control protocol has no authentication, authorization, or transport
encryption. Do not expose it directly to an untrusted network. Bind it to
loopback, a pod-local interface, or another network boundary you control.

The project also lacks the compatibility guarantees, soak testing, packaging,
and operational hardening expected of a production logging agent. Some ideas in
the design are more mature than others, and the interfaces may change.

Still, the core combination appears useful: live local policy, bounded memory,
explicit overflow, no surprise persistence, and compatibility with ordinary
stdout collection. The experiment is here to find out how far that combination
can go without losing its small-tool character.

## Direction

Good future work should preserve the project's temperament:

- add authentication and encryption without turning the edge into a control
  platform;
- make durability and remote export explicit, bounded choices;
- improve failure accounting and backpressure behavior;
- add deployment packaging and longer-running tests;
- keep configuration legible and operational changes reversible;
- resist features that require unbounded queues, hidden persistence, or a
  mandatory central service.

If `edgelog` becomes useful, it should be because it remains a welcome house
guest: quiet by default, bounded in appetite, candid about loss, and easy to ask
to leave.
