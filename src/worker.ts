/**
 * fleet-metrics-cron — Aggregates observability metrics for the SuperInstance fleet.
 *
 * Runs every 5 minutes via Cloudflare Cron Trigger. Queries the D1 `spans` and
 * `events` tables, computes latency percentiles, error rates, throughput, budget
 * consumption, and anomaly detection. Writes results to KV with 10-min TTL.
 */

// ─── Types ────────────────────────────────────────────────────────────────────

export interface Env {
  DB: D1Database;
  KV: KVNamespace;
}

interface Anomaly {
  id: string;
  rule: string;
  severity: 'warning' | 'critical';
  worker?: string;
  message: string;
  current_value: number;
  threshold: number;
  baseline?: number;
  detected_at: string;
  window: string;
}

// ─── SQL Queries ──────────────────────────────────────────────────────────────

/** P50/P95/P99 latency via ROW_NUMBER approximation */
const LATENCY_QUERY = `
WITH ranked AS (
  SELECT duration_ms,
         ROW_NUMBER() OVER (ORDER BY duration_ms) AS rn,
         COUNT(*) OVER () AS total
  FROM spans
  WHERE origin_worker = ?1
    AND ts >= ?2 AND ts < ?3
    AND operation != 'cron.tick'
)
SELECT
  AVG(CASE WHEN rn BETWEEN FLOOR(total * 0.48) AND CEIL(total * 0.52) THEN duration_ms END) AS p50,
  AVG(CASE WHEN rn BETWEEN FLOOR(total * 0.73) AND CEIL(total * 0.77) THEN duration_ms END) AS p75,
  AVG(CASE WHEN rn BETWEEN FLOOR(total * 0.88) AND CEIL(total * 0.92) THEN duration_ms END) AS p90,
  AVG(CASE WHEN rn BETWEEN FLOOR(total * 0.93) AND CEIL(total * 0.97) THEN duration_ms END) AS p95,
  AVG(CASE WHEN rn BETWEEN FLOOR(total * 0.98) AND CEIL(total * 0.995) THEN duration_ms END) AS p99,
  MIN(duration_ms) AS min_ms,
  MAX(duration_ms) AS max_ms,
  AVG(duration_ms) AS avg_ms,
  COUNT(*) AS sample_count
FROM ranked`;

/** Latency percentiles broken down by operation */
const LATENCY_BY_OP_QUERY = `
WITH ranked AS (
  SELECT duration_ms, operation,
         ROW_NUMBER() OVER (PARTITION BY operation ORDER BY duration_ms) AS rn,
         COUNT(*) OVER (PARTITION BY operation) AS total
  FROM spans
  WHERE origin_worker = ?1
    AND ts >= ?2 AND ts < ?3
    AND operation != 'cron.tick'
)
SELECT
  operation,
  AVG(CASE WHEN rn BETWEEN FLOOR(total * 0.48) AND CEIL(total * 0.52) THEN duration_ms END) AS p50,
  AVG(CASE WHEN rn BETWEEN FLOOR(total * 0.93) AND CEIL(total * 0.97) THEN duration_ms END) AS p95,
  AVG(CASE WHEN rn BETWEEN FLOOR(total * 0.98) AND CEIL(total * 0.995) THEN duration_ms END) AS p99,
  MAX(total) AS count
FROM ranked
GROUP BY operation`;

/** Error counts + rate for a worker in a window */
const ERROR_RATE_QUERY = `
SELECT
  COUNT(*) AS total_requests,
  SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_count,
  SUM(CASE WHEN status = 'timeout' THEN 1 ELSE 0 END) AS timeout_count,
  SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) AS ok_count,
  CAST(SUM(CASE WHEN status != 'ok' THEN 1 ELSE 0 END) AS REAL) / COUNT(*) AS error_rate
FROM spans
WHERE origin_worker = ?1
  AND ts >= ?2 AND ts < ?3`;

/** Error breakdown by type */
const ERROR_TYPE_QUERY = `
SELECT
  COALESCE(
    JSON_EXTRACT(metadata, '$.error_type'),
    CASE
      WHEN error_message LIKE '%D1%' THEN 'D1_ERROR'
      WHEN error_message LIKE '%timeout%' OR status = 'timeout' THEN 'TIMEOUT'
      WHEN error_message LIKE '%5[0-9][0-9]%' THEN 'UPSTREAM_5XX'
      ELSE 'UNKNOWN'
    END
  ) AS error_type,
  COUNT(*) AS count
FROM spans
WHERE origin_worker = ?1
  AND ts >= ?2 AND ts < ?3
  AND status != 'ok'
GROUP BY error_type`;

/** Total throughput for a worker */
const THROUGHPUT_QUERY = `
SELECT
  COUNT(*) AS request_count,
  CAST(COUNT(*) AS REAL) / 300.0 AS requests_per_second
FROM spans
WHERE origin_worker = ?1
  AND ts >= ?2 AND ts < ?3`;

/** Throughput breakdown by target */
const THROUGHPUT_TARGET_QUERY = `
SELECT target, COUNT(*) AS count
FROM spans
WHERE origin_worker = ?1
  AND ts >= ?2 AND ts < ?3
  AND target IS NOT NULL
GROUP BY target`;

/** Budget consumption from events table */
const BUDGET_QUERY = `
SELECT
  SUM(CASE
    WHEN type = 'budget.consume' THEN CAST(JSON_EXTRACT(payload, '$.amount') AS REAL)
    ELSE 0
  END) AS consumed_window,
  COUNT(*) AS event_count
FROM events
WHERE domain = 'budget'
  AND ts >= ?1 AND ts < ?2`;

/** 7-day rolling baselines per worker */
const BASELINE_QUERY = `
WITH ranked AS (
  SELECT duration_ms, origin_worker, status,
         ROW_NUMBER() OVER (PARTITION BY origin_worker ORDER BY duration_ms) AS rn,
         COUNT(*) OVER (PARTITION BY origin_worker) AS total
  FROM spans
  WHERE ts >= datetime('now', '-7 days')
    AND operation != 'cron.tick'
)
SELECT
  origin_worker,
  AVG(CASE WHEN rn BETWEEN FLOOR(total*0.48) AND CEIL(total*0.52) THEN duration_ms END) AS latency_p50,
  AVG(CASE WHEN rn BETWEEN FLOOR(total*0.93) AND CEIL(total*0.97) THEN duration_ms END) AS latency_p95,
  AVG(CASE WHEN rn BETWEEN FLOOR(total*0.98) AND CEIL(total*0.995) THEN duration_ms END) AS latency_p99,
  CAST(SUM(CASE WHEN status != 'ok' THEN 1 ELSE 0 END) AS REAL) / MAX(total) AS error_rate
FROM ranked
GROUP BY origin_worker`;

/** Cooldown check — count recent anomaly events for a rule+worker */
const COOLDOWN_QUERY = `
SELECT COUNT(*) AS cnt
FROM events
WHERE domain = 'observability'
  AND type = 'anomaly.detected'
  AND JSON_EXTRACT(payload, '$.rule') = ?1
  AND JSON_EXTRACT(payload, '$.worker') = ?2
  AND ts > datetime('now', ?3)`;

/** A5 silence check — was this worker active in the previous window? */
const PREVIOUS_WINDOW_QUERY = `
SELECT COUNT(*) AS cnt
FROM spans
WHERE origin_worker = ?1
  AND ts >= ?2 AND ts < ?3`;

// ─── Helpers ──────────────────────────────────────────────────────────────────

const ALL_WORKERS = [
  'fleet-edge', 'fleet-vector-api', 'fleet-budget', 'fleet-event-router',
  'fleet-health', 'fleet-orchestrator', 'fleet-escalation', 'fleet-balance',
  'fleet-notify', 'fleet-digest', 'fleet-heartbeat', 'fleet-ingest',
  'fleet-transform', 'fleet-metrics-cron', 'fleet-gateway',
];

const TTL = 600; // 10 minutes

function computeWindowKey(ts: number): string {
  const d = new Date(ts);
  d.setSeconds(0);
  d.setMilliseconds(0);
  d.setMinutes(Math.floor(d.getMinutes() / 5) * 5);
  return d.toISOString().replace(/\.\d{3}Z$/, 'Z');
}

function round1(v: number | null | undefined): number | null {
  return v != null ? Math.round(v * 10) / 10 : null;
}

function round2(v: number | null | undefined): number | null {
  return v != null ? Math.round(v * 100) / 100 : null;
}

// ─── Per-Worker Metrics ──────────────────────────────────────────────────────

async function computeAndStoreWorkerMetrics(
  env: Env, worker: string, ws: string, we: string, windowKey: string,
): Promise<{ errMetrics: any; latMetrics: any; thrMetrics: any }> {
  const [latency, latencyByOp, errors, errorTypes, throughput, throughputByTarget] =
    await Promise.all([
      env.DB.prepare(LATENCY_QUERY).bind(worker, ws, we).first(),
      env.DB.prepare(LATENCY_BY_OP_QUERY).bind(worker, ws, we).all(),
      env.DB.prepare(ERROR_RATE_QUERY).bind(worker, ws, we).first(),
      env.DB.prepare(ERROR_TYPE_QUERY).bind(worker, ws, we).all(),
      env.DB.prepare(THROUGHPUT_QUERY).bind(worker, ws, we).first(),
      env.DB.prepare(THROUGHPUT_TARGET_QUERY).bind(worker, ws, we).all(),
    ]);

  // ── Latency ──
  let latMetrics: any = null;
  if (latency && (latency as any).sample_count > 0) {
    const l = latency as Record<string, any>;
    const latencyPayload = {
      worker,
      window_start: ws,
      window_end: we,
      sample_count: l.sample_count,
      percentiles: {
        p50: round1(l.p50),
        p75: round1(l.p75),
        p90: round1(l.p90),
        p95: round1(l.p95),
        p99: round1(l.p99),
      },
      min: round1(l.min_ms),
      max: round1(l.max_ms),
      avg: round1(l.avg_ms),
      by_operation: Object.fromEntries(
        (latencyByOp as any).results.map((r: any) => [r.operation, {
          p50: round1(r.p50),
          p95: round1(r.p95),
          p99: round1(r.p99),
          count: r.count,
        }])
      ),
      ts: we,
    };
    await env.KV.put(
      `metrics:latency:${worker}:${windowKey}`,
      JSON.stringify(latencyPayload),
      { expirationTtl: TTL },
    );
  }

  // ── Errors ──
  const e = errors as Record<string, any> | null;
  const errPayload = {
    worker,
    window_start: ws,
    window_end: we,
    total_requests: e?.total_requests ?? 0,
    total_errors: (e?.error_count ?? 0) + (e?.timeout_count ?? 0),
    error_rate: e?.error_rate ?? 0,
    by_status: {
      ok: e?.ok_count ?? 0,
      error: e?.error_count ?? 0,
      timeout: e?.timeout_count ?? 0,
    },
    by_error_type: Object.fromEntries(
      (errorTypes as any).results.map((r: any) => [r.error_type, r.count])
    ),
    ts: we,
  };
  await env.KV.put(
    `metrics:errors:${worker}:${windowKey}`,
    JSON.stringify(errPayload),
    { expirationTtl: TTL },
  );

  // ── Throughput ──
  const t = throughput as Record<string, any> | null;
  const thrPayload = {
    worker,
    window_start: ws,
    window_end: we,
    request_count: t?.request_count ?? 0,
    requests_per_second: round2(t?.requests_per_second ?? 0),
    by_target: Object.fromEntries(
      (throughputByTarget as any).results.map((r: any) => [r.target, r.count])
    ),
    ts: we,
  };
  await env.KV.put(
    `metrics:throughput:${worker}:${windowKey}`,
    JSON.stringify(thrPayload),
    { expirationTtl: TTL },
  );

  // Read back for downstream consumers (overview, anomalies)
  const [errMetrics, _latMetrics, thrMetrics] = await Promise.all([
    env.KV.get(`metrics:errors:${worker}:${windowKey}`, 'json'),
    env.KV.get(`metrics:latency:${worker}:${windowKey}`, 'json'),
    env.KV.get(`metrics:throughput:${worker}:${windowKey}`, 'json'),
  ]);
  latMetrics = _latMetrics;

  return { errMetrics, latMetrics, thrMetrics };
}

// ─── Budget Metrics ───────────────────────────────────────────────────────────

async function computeBudgetMetrics(
  env: Env, ws: string, we: string, windowKey: string,
): Promise<void> {
  const result = await env.DB.prepare(BUDGET_QUERY).bind(ws, we).first();
  const r = result as Record<string, any> | null;

  const consumedWindow = r?.consumed_window ?? 0;
  const consumptionRatePerMin = round6(consumedWindow / 5);
  const projectedDaily = round4(consumptionRatePerMin * 1440); // 1440 min/day

  const payload = {
    window_start: ws,
    window_end: we,
    budget_consumed_window: round6(consumedWindow),
    consumption_rate_per_min: consumptionRatePerMin,
    projected_daily_total: projectedDaily,
    event_count: r?.event_count ?? 0,
    ts: we,
  };

  await env.KV.put(
    `metrics:budget:${windowKey}`,
    JSON.stringify(payload),
    { expirationTtl: TTL },
  );
}

function round6(v: number): number { return Math.round(v * 1_000_000) / 1_000_000; }
function round4(v: number): number { return Math.round(v * 10_000) / 10_000; }

// ─── Fleet Overview ───────────────────────────────────────────────────────────

async function computeOverview(
  env: Env,
  workerData: Array<{ name: string; err: any; lat: any; thr: any }>,
  baselines: Record<string, any>,
  windowKey: string,
  ws: string,
  we: string,
): Promise<void> {
  const totalRequests = workerData.reduce((s, w) => s + (w.thr?.request_count ?? 0), 0);
  const totalErrors = workerData.reduce((s, w) => s + (w.err?.total_errors ?? 0), 0);
  const fleetErrorRate = totalRequests > 0 ? round4(totalErrors / totalRequests) : 0;

  // Aggregate percentiles
  const p50s = workerData.map(w => w.lat?.percentiles?.p50).filter(Boolean) as number[];
  const p95s = workerData.map(w => w.lat?.percentiles?.p95).filter(Boolean) as number[];
  const p99s = workerData.map(w => w.lat?.percentiles?.p99).filter(Boolean) as number[];
  const median = (arr: number[]) => {
    const s = [...arr].sort((a, b) => a - b);
    const mid = Math.floor(s.length / 2);
    return s.length ? round1(s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2) : null;
  };

  const budgetRaw = await env.KV.get(`metrics:budget:${windowKey}`, 'json') as any;
  const anomaliesRaw = await env.KV.get(`metrics:anomalies:${windowKey}`, 'json') as any;

  const overview = {
    $schema: 'fleet-observability-dashboard-v1',
    generated_at: new Date().toISOString(),
    window: { start: ws, end: we },
    fleet_summary: {
      total_requests: totalRequests,
      total_errors: totalErrors,
      fleet_error_rate: fleetErrorRate,
      fleet_p50_ms: median(p50s),
      fleet_p95_ms: median(p95s),
      fleet_p99_ms: p99s.length ? round1(Math.max(...p99s)) : null,
      active_workers: ALL_WORKERS.length,
      healthy_workers: workerData.filter(w => (w.err?.error_rate ?? 0) < 0.05).length,
      degraded_workers: workerData.filter(w => (w.err?.error_rate ?? 0) >= 0.05).length,
      budget_consumed_pct: round2((budgetRaw?.budget_consumed_window ?? 0) * 100),
      budget_projected_pct: round2((budgetRaw?.projected_daily_total ?? 0) * 100),
      budget_status: (budgetRaw?.projected_daily_total ?? 0) > 0.9 ? 'critical'
        : (budgetRaw?.projected_daily_total ?? 0) > 0.7 ? 'warning' : 'nominal',
    },
    anomalies: anomaliesRaw
      ? {
          active_count: anomaliesRaw.anomaly_count,
          critical: anomaliesRaw.anomalies.filter((a: Anomaly) => a.severity === 'critical'),
          warnings: anomaliesRaw.anomalies.filter((a: Anomaly) => a.severity === 'warning'),
        }
      : { active_count: 0, critical: [], warnings: [] },
    workers: workerData.map(w => {
      const bl = baselines[w.name] ?? {};
      return {
        name: w.name,
        status: (w.err?.error_rate ?? 0) > 0.05 ? 'degraded' : 'healthy',
        requests: w.thr?.request_count ?? 0,
        error_rate: w.err?.error_rate ?? 0,
        latency: {
          p50: w.lat?.percentiles?.p50 ?? null,
          p95: w.lat?.percentiles?.p95 ?? null,
          p99: w.lat?.percentiles?.p99 ?? null,
          baseline_p95: bl.latency_p95 ?? null,
          p95_vs_baseline: (w.lat?.percentiles?.p95 && bl.latency_p95)
            ? round2(w.lat.percentiles.p95 / bl.latency_p95) : null,
        },
        top_errors: Object.entries(w.err?.by_error_type ?? {})
          .sort(([, a]: any, [, b]: any) => (b as number) - (a as number))
          .slice(0, 3)
          .map(([type, count]) => ({ type, count })),
        throughput_rps: w.thr?.requests_per_second ?? 0,
      };
    }),
  };

  await env.KV.put(
    `metrics:overview:${windowKey}`,
    JSON.stringify(overview),
    { expirationTtl: TTL },
  );
}

// ─── Anomaly Detection ────────────────────────────────────────────────────────

async function checkCooldown(
  env: Env, rule: string, worker: string | undefined, cooldownMin: number,
): Promise<boolean> {
  const workerFilter = worker ?? '';
  const result = await env.DB.prepare(COOLDOWN_QUERY)
    .bind(rule, workerFilter, `-${cooldownMin} minutes`)
    .first();
  return ((result as any)?.cnt ?? 0) > 0;
}

async function detectAnomalies(
  env: Env,
  workerData: Array<{ name: string; err: any; lat: any; thr: any }>,
  baselines: Record<string, any>,
  windowKey: string,
): Promise<void> {
  const anomalies: Anomaly[] = [];
  const now = new Date().toISOString();

  for (const w of workerData) {
    const bl = baselines[w.name] ?? {};
    const err = w.err;
    const lat = w.lat;

    // A1: High Error Rate (>5%)
    if (err && err.error_rate > 0.05) {
      if (!await checkCooldown(env, 'high_error_rate', w.name, 15)) {
        anomalies.push({
          id: `A1-${w.name}-${windowKey}`,
          rule: 'high_error_rate',
          severity: 'critical',
          worker: w.name,
          message: `${w.name} error rate ${(err.error_rate * 100).toFixed(1)}% exceeds 5% threshold`,
          current_value: err.error_rate,
          threshold: 0.05,
          baseline: bl.error_rate,
          detected_at: now,
          window: windowKey,
        });
      }
    }

    // A2: Latency Spike (p95 > 2× baseline)
    if (lat?.percentiles?.p95 && bl.latency_p95 && lat.percentiles.p95 > 2 * bl.latency_p95) {
      if (!await checkCooldown(env, 'latency_spike_p95', w.name, 30)) {
        anomalies.push({
          id: `A2-${w.name}-${windowKey}`,
          rule: 'latency_spike_p95',
          severity: lat.percentiles.p95 > (bl.latency_p99 ?? Infinity) * 5 ? 'critical' : 'warning',
          worker: w.name,
          message: `${w.name} p95 ${lat.percentiles.p95}ms is ${(lat.percentiles.p95 / bl.latency_p95).toFixed(1)}x baseline (${bl.latency_p95}ms)`,
          current_value: lat.percentiles.p95,
          threshold: round1(2 * bl.latency_p95),
          baseline: bl.latency_p95,
          detected_at: now,
          window: windowKey,
        });
      }
    }

    // A4: Error Rate Trend (>3× previous window)
    if (err) {
      const prevWindow = computeWindowKey(new Date(windowKey).getTime() - 5 * 60 * 1000);
      const prevErr = await env.KV.get(`metrics:errors:${w.name}:${prevWindow}`, 'json') as any;
      if (prevErr?.error_rate != null && prevErr.error_rate > 0
          && err.error_rate > 3 * prevErr.error_rate) {
        if (!await checkCooldown(env, 'error_rate_trend', w.name, 30)) {
          anomalies.push({
            id: `A4-${w.name}-${windowKey}`,
            rule: 'error_rate_trend',
            severity: 'warning',
            worker: w.name,
            message: `${w.name} error rate jumped from ${(prevErr.error_rate * 100).toFixed(2)}% to ${(err.error_rate * 100).toFixed(2)}% (>3×)`,
            current_value: err.error_rate,
            threshold: round4(3 * prevErr.error_rate),
            baseline: prevErr.error_rate,
            detected_at: now,
            window: windowKey,
          });
        }
      }
    }

    // A5: Worker Silent (no events in current or previous window)
    if ((!err || err.total_requests === 0) && (!lat || lat.sample_count === 0)) {
      // Check previous window for activity
      const prevWindow = computeWindowKey(new Date(windowKey).getTime() - 5 * 60 * 1000);
      const prevThr = await env.KV.get(`metrics:throughput:${w.name}:${prevWindow}`, 'json') as any;
      if (prevThr && prevThr.request_count > 0) {
        if (!await checkCooldown(env, 'worker_silent', w.name, 60)) {
          anomalies.push({
            id: `A5-${w.name}-${windowKey}`,
            rule: 'worker_silent',
            severity: 'warning',
            worker: w.name,
            message: `${w.name} produced 0 spans this window (had ${prevThr.request_count} requests last window)`,
            current_value: 0,
            threshold: 0,
            detected_at: now,
            window: windowKey,
          });
        }
      }
    }

    // A7: Tail Latency Explosion (p99 > 5× baseline)
    if (lat?.percentiles?.p99 && bl.latency_p99 && lat.percentiles.p99 > 5 * bl.latency_p99) {
      if (!await checkCooldown(env, 'tail_latency_explosion', w.name, 15)) {
        anomalies.push({
          id: `A7-${w.name}-${windowKey}`,
          rule: 'tail_latency_explosion',
          severity: 'critical',
          worker: w.name,
          message: `${w.name} p99 ${lat.percentiles.p99}ms is ${(lat.percentiles.p99 / bl.latency_p99).toFixed(1)}x baseline (${bl.latency_p99}ms)`,
          current_value: lat.percentiles.p99,
          threshold: round1(5 * bl.latency_p99),
          baseline: bl.latency_p99,
          detected_at: now,
          window: windowKey,
        });
      }
    }

    // A8: Timeout Surge (>10 timeouts in 5-min window)
    if (err?.by_status?.timeout && err.by_status.timeout > 10) {
      if (!await checkCooldown(env, 'timeout_surge', w.name, 30)) {
        anomalies.push({
          id: `A8-${w.name}-${windowKey}`,
          rule: 'timeout_surge',
          severity: 'warning',
          worker: w.name,
          message: `${w.name} has ${err.by_status.timeout} timeouts in 5-min window`,
          current_value: err.by_status.timeout,
          threshold: 10,
          detected_at: now,
          window: windowKey,
        });
      }
    }
  }

  // A6: Cascading Failure (≥3 workers with error_rate > 2%)
  const failingWorkers = workerData.filter(w => (w.err?.error_rate ?? 0) > 0.02);
  if (failingWorkers.length >= 3) {
    if (!await checkCooldown(env, 'cascading_failure', undefined, 15)) {
      anomalies.push({
        id: `A6-${windowKey}`,
        rule: 'cascading_failure',
        severity: 'critical',
        message: `${failingWorkers.length} workers have error_rate > 2% simultaneously — possible cascading failure`,
        current_value: failingWorkers.length,
        threshold: 3,
        detected_at: now,
        window: windowKey,
      });
    }
  }

  // A3: Budget Drain (consumption_rate_per_min > 0.001 = 10% daily per minute)
  const budgetMetrics = await env.KV.get(`metrics:budget:${windowKey}`, 'json') as any;
  if (budgetMetrics?.consumption_rate_per_min > 0.001) {
    if (!await checkCooldown(env, 'budget_drain', undefined, 15)) {
      anomalies.push({
        id: `A3-${windowKey}`,
        rule: 'budget_drain',
        severity: 'critical',
        message: `Budget consuming ${(budgetMetrics.consumption_rate_per_min * 100).toFixed(2)}%/min — projected daily ${(budgetMetrics.projected_daily_total * 100).toFixed(0)}%`,
        current_value: budgetMetrics.consumption_rate_per_min,
        threshold: 0.001,
        detected_at: now,
        window: windowKey,
      });
    }
  }

  // Store anomalies to KV
  if (anomalies.length > 0) {
    await env.KV.put(
      `metrics:anomalies:${windowKey}`,
      JSON.stringify({
        window: windowKey,
        anomaly_count: anomalies.length,
        critical_count: anomalies.filter(a => a.severity === 'critical').length,
        warning_count: anomalies.filter(a => a.severity === 'warning').length,
        anomalies,
        ts: now,
      }),
      { expirationTtl: TTL },
    );

    // Persist to D1 for historical tracking + cooldown
    const stmt = env.DB.prepare(`
      INSERT INTO events (id, domain, type, aggregate, payload, actor, ts, trace_id, origin_worker)
      VALUES (?1, 'observability', 'anomaly.detected', 'anomaly', ?2, 'fleet-metrics-cron',
              strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'cron', 'fleet-metrics-cron')
    `);
    for (const anomaly of anomalies) {
      await stmt.bind(crypto.randomUUID(), JSON.stringify(anomaly)).run();
    }
  }
}

// ─── Baseline Computation ─────────────────────────────────────────────────────

async function updateBaselines(env: Env): Promise<void> {
  const result = await env.DB.prepare(BASELINE_QUERY).all();
  const workers: Record<string, any> = {};

  for (const row of (result as any).results) {
    workers[row.origin_worker] = {
      latency_p50: round1(row.latency_p50),
      latency_p95: round1(row.latency_p95),
      latency_p99: round1(row.latency_p99),
      error_rate: round4(row.error_rate),
    };
  }

  await env.KV.put(
    'metrics:baselines',
    JSON.stringify({
      period: '7d',
      updated: new Date().toISOString(),
      workers,
    }),
    { expirationTtl: 86400 }, // 24 hours
  );
}

// ─── Main Scheduled Handler ───────────────────────────────────────────────────

export default {
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    const windowEnd = new Date(event.scheduledTime);
    windowEnd.setMilliseconds(0);
    windowEnd.setSeconds(0);
    windowEnd.setMinutes(Math.floor(windowEnd.getMinutes() / 5) * 5);
    const windowStart = new Date(windowEnd.getTime() - 5 * 60 * 1000);

    const ws = windowStart.toISOString().replace(/\.\d{3}Z$/, 'Z');
    const we = windowEnd.toISOString().replace(/\.\d{3}Z$/, 'Z');
    const windowKey = ws;

    // Process workers in parallel batches of 5 (D1 connection limit)
    const workerResults: Array<{ name: string; err: any; lat: any; thr: any }> = [];
    for (let i = 0; i < ALL_WORKERS.length; i += 5) {
      const batch = ALL_WORKERS.slice(i, i + 5);
      const batchResults = await Promise.all(
        batch.map(async (name) => {
          const { errMetrics, latMetrics, thrMetrics } =
            await computeAndStoreWorkerMetrics(env, name, ws, we, windowKey);
          return { name, err: errMetrics, lat: latMetrics, thr: thrMetrics };
        })
      );
      workerResults.push(...batchResults);
    }

    // Budget metrics
    await computeBudgetMetrics(env, ws, we, windowKey);

    // Load baselines
    const baselinesRaw = await env.KV.get('metrics:baselines', 'json') as any;
    const baselines: Record<string, any> = baselinesRaw?.workers ?? {};

    // Anomaly detection
    await detectAnomalies(env, workerResults, baselines, windowKey);

    // Fleet overview (reads from KV)
    await computeOverview(env, workerResults, baselines, windowKey, ws, we);

    // Update baselines daily at midnight UTC
    if (windowEnd.getUTCHours() === 0 && windowEnd.getUTCMinutes() === 0) {
      await updateBaselines(env);
    }
  },
};
