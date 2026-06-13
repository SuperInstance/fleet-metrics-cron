# fleet-metrics-cron

A Cloudflare Worker cron trigger that aggregates observability metrics for the SuperInstance fleet every 5 minutes. It queries D1 trace spans and event tables, computes latency percentiles (P50/P75/P90/P95/P99), error rates, throughput, budget consumption, and runs multi-rule anomaly detection against 7-day rolling baselines. Results are published to KV with a 10-minute TTL for downstream dashboards and alerting.

## Why It Matters

Distributed systems fail silently without observability. This worker is the fleet's nervous system: it transforms raw D1 span rows into actionable percentiles, detects anomalies (error spikes, latency regressions, cascading failures, budget drain), and writes structured summaries to KV so any consumer can read fleet health in a single lookup. Without it, operators are blind to degradation until users complain.

## How It Works

### Percentile Approximation via ROW_NUMBER

Instead of importing a percentile library, the worker uses SQL window functions to approximate percentiles directly in D1:

```
p_α ≈ AVG(CASE WHEN rn BETWEEN FLOOR(N·(α-δ)) AND CEIL(N·(α+δ)) THEN duration_ms END)
```

where `rn = ROW_NUMBER() OVER (ORDER BY duration_ms)`, `N = COUNT(*)`, and `δ` is a small band (±0.02) that averages rows near the target quantile. This is O(N log N) due to the sort inside the window function.

### Health Scoring Model

Each worker is classified as healthy or degraded based on:

```
status = error_rate < 0.05 ? "healthy" : "degraded"
```

The fleet overview aggregates this into a count: `healthy_workers` vs `degraded_workers`.

### Anomaly Detection Rules

The engine implements 8 anomaly rules, each with cooldown logic to prevent alert fatigue:

| Rule | Condition | Severity | Cooldown |
|------|-----------|----------|----------|
| A1: High Error Rate | `error_rate > 0.05` | Critical | 15 min |
| A2: Latency Spike | `p95 > 2 × baseline_p95` | Warning/Critical | 30 min |
| A3: Budget Drain | `consumption_rate > 0.001/min` | Critical | 15 min |
| A4: Error Trend | `error_rate > 3 × prev_window` | Warning | 30 min |
| A5: Worker Silent | `spans = 0` (was active last window) | Warning | 60 min |
| A6: Cascading Failure | `≥ 3 workers with error_rate > 0.02` | Critical | 15 min |
| A7: Tail Latency | `p99 > 5 × baseline_p99` | Critical | 15 min |
| A8: Timeout Surge | `timeouts > 10` in 5-min window | Warning | 30 min |

Cooldown is enforced by querying `events` for recent `anomaly.detected` entries matching the rule and worker within the cooldown window.

### Time Complexity

- **Per-worker metrics**: O(N log N) for percentile queries where N = span count in window
- **Anomaly detection**: O(W) where W = number of workers (15), with O(1) KV lookups for baselines
- **Total per tick**: O(W · N log N), dominated by D1 SQL execution

### Space Complexity

- KV entries: O(W) per window key (3 per worker: latency, errors, throughput) + 1 overview + anomalies
- D1 writes: O(A) where A = anomalies detected (for cooldown persistence)

## Quick Start

```bash
# Install dependencies
npm install

# Local dev (requires wrangler)
npm run dev

# Deploy to Cloudflare
npm run deploy

# Tail logs
npm run tail
```

### Required Bindings

```toml
# wrangler.toml
[[d1_databases]]
binding = "DB"
database_name = "fleet-events"

[[kv_namespaces]]
binding = "KV"

[triggers]
crons = ["*/5 * * * *"]
```

## API

The worker exposes no HTTP endpoints — it runs purely as a `scheduled` handler. Consumers read from KV:

```
metrics:overview:{windowKey}       — Fleet-wide dashboard payload
metrics:latency:{worker}:{windowKey}  — Per-worker latency percentiles
metrics:errors:{worker}:{windowKey}   — Per-worker error breakdown
metrics:throughput:{worker}:{windowKey} — Per-worker request volume
metrics:budget:{windowKey}            — Budget consumption metrics
metrics:anomalies:{windowKey}         — Active anomalies for the window
metrics:baselines                    — 7-day rolling baselines (24h TTL)
```

### Window Key Format

Window keys are ISO timestamps truncated to 5-minute boundaries:
```
2024-12-10T14:05:00Z
```

## Architecture Notes

This worker embodies the **γ + η = C** principle: it is the **feedback controller (γ)** that closes the observability loop for the fleet. The fleet's executors (edge workers, APIs, orchestrators) are the **plant (η)**. Without this controller, the fleet operates open-loop — no detection of degradation, no early warning on budget exhaustion, no cascading failure prevention. The 5-minute cadence is the sampling period `T_s` of the discrete-time control system; the anomaly rules are the transfer function `C(z)` that maps observed metrics to corrective signals.

### Processing Pipeline

```
D1 spans/events → SQL aggregation → KV metrics → anomaly rules → KV anomalies → fleet overview
```

Workers are processed in batches of 5 (D1 connection limit). All KV writes use `expirationTtl: 600` (10 min) so stale windows auto-expire.

### Baseline Updates

7-day rolling baselines (P50/P95/P99 latency + error rate per worker) are recomputed at UTC midnight. These baselines feed the A2 and A7 anomaly rules.

## References

- **Percentile approximation**: Cormode, G., et al. "A unified approach to constrained sampling." *ACM SIGMOD*, 2012.
- **Anomaly detection in time series**: Chandola, V., et al. "Anomaly detection: A survey." *ACM Computing Surveys* 41.3 (2009): 1–58.
- **Cloudflare D1 Window Functions**: [developers.cloudflare.com/d1](https://developers.cloudflare.com/d1/)
- **SRE alerting best practices**: Beyer, B., et al. *Site Reliability Engineering.* O'Reilly, 2016. Chapter 6: Monitoring.

## License

MIT
