# fleet-metrics-cron

Cloudflare Worker that aggregates observability metrics for the SuperInstance fleet of 15 Workers. Runs every 5 minutes via Cron Trigger.

## What It Does

Every 5 minutes, `fleet-metrics-cron`:

1. **Queries D1** — Reads the `spans` and `events` tables for the last 5-minute window for all 15 fleet workers
2. **Computes metrics** — Latency percentiles (p50/p75/p90/p95/p99), error rates, throughput, budget consumption
3. **Detects anomalies** — Runs 8 anomaly rules (high error rate, latency spikes, budget drain, cascading failures, etc.)
4. **Writes to KV** — Stores results under `metrics:*` keys with 10-minute TTL
5. **Updates baselines** — Recomputes 7-day rolling baselines daily at midnight UTC

### KV Key Patterns

| Key | TTL | Content |
|---|---|---|
| `metrics:latency:{worker}:{window}` | 10 min | Latency percentiles per worker |
| `metrics:errors:{worker}:{window}` | 10 min | Error counts and rates per worker |
| `metrics:throughput:{worker}:{window}` | 10 min | Request counts per worker |
| `metrics:budget:{window}` | 10 min | Budget consumption rates |
| `metrics:overview:{window}` | 10 min | Fleet-wide dashboard summary |
| `metrics:anomalies:{window}` | 10 min | Active anomaly flags |
| `metrics:baselines` | 24 h | 7-day rolling baselines per worker |

Where `{window}` is an ISO 8601 timestamp truncated to the 5-minute boundary (e.g., `2026-06-10T03:30:00Z`).

### Anomaly Rules

| Rule | Condition | Severity |
|---|---|---|
| A1 | error_rate > 5% | critical |
| A2 | p95 > 2× baseline | warning |
| A3 | budget drain > 10%/min | critical |
| A4 | error rate > 3× previous window | warning |
| A5 | Worker silent (no spans, was active before) | warning |
| A6 | ≥3 workers degraded simultaneously | critical |
| A7 | p99 > 5× baseline | critical |
| A8 | >10 timeouts in 5-min window | warning |

Each rule has a cooldown to prevent alert storms.

## Prerequisites

- D1 database named `fleet-events` with `spans` and `events` tables (migrations V006/V007 applied)
- KV namespace named `fleet-orchestrator-kv`

## Setup

```bash
# Install dependencies
npm install

# Create D1 database (if not already created)
npx wrangler d1 create fleet-events
# → copy database_id into wrangler.toml

# Create KV namespace (if not already created)
npx wrangler kv namespace create fleet-orchestrator-kv
# → copy id into wrangler.toml

# Apply migrations (from the main fleet repo)
npx wrangler d1 execute fleet-events --file=migrations/V006__add_trace_columns.sql
npx wrangler d1 execute fleet-events --file=migrations/V007__create_spans_table.sql
```

## Development

```bash
# Local dev with cron trigger simulation
npx wrangler dev --test-scheduled
# Then: curl "http://localhost:8787/__scheduled?cron=*/5+*+*+*+*"

# Type check
npm run typecheck
```

## Deploy

```bash
npm run deploy
```

## Monitoring

```bash
# Live tail logs
npm run tail
```

## Architecture

```
15 Workers → D1 (spans + events) → fleet-metrics-cron (every 5 min) → KV
                                                                    ↓
                                                            Dashboard API
```

Workers write spans to D1 as fire-and-forget (`.run()` without `await`). This cron reads them back in batches of 5 (D1 connection limit), aggregates, and writes to KV for dashboard consumption.
