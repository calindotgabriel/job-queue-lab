// AFTER — a durable Postgres job queue: atomic claim, retry with backoff, reaper.
//
//   docker compose up -d
//   npm install
//   npm run after        
//
// The harness enqueues a mix of jobs, runs 3 workers + a reaper for ~6s,
// then prints the final job states and the handler call log.

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { handlers, handlerLog } from "./handlers.ts";

const here = dirname(fileURLToPath(import.meta.url));
const DATABASE_URL =
  process.env.DATABASE_URL ?? "postgres://postgres:lab@localhost:5434/lab";
const pool = new pg.Pool({ connectionString: DATABASE_URL, max: 20 });

interface Job {
  id: string;
  type: string;
  payload: Record<string, unknown>;
  run_at: Date;
  status: string;
  attempts: number;
  max_attempts: number;
  locked_at: Date | null;
  last_error: string | null;
}

const BATCH = 5;
const VISIBILITY_TIMEOUT_SEC = 30;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function enqueue(
  type: string,
  payload: unknown,
  runAt: Date = new Date(),
): Promise<void> {
  await pool.query(
    `INSERT INTO jobs (type, payload, run_at) VALUES ($1, $2, $3)`,
    [type, JSON.stringify(payload), runAt],
  );
}

// ===========================================================================
//  1 — the claim is atomic
// ---------------------------------------------------------------------------
//  A plain SELECT then UPDATE lets several workers read the same rows before
//  any of them marks the rows taken, so jobs run 2–3 times. One statement fixes it.
// ===========================================================================
async function claimJobs(): Promise<Job[]> {
  const { rows } = await pool.query<Job>(
    `UPDATE jobs SET status = 'running', locked_at = now()
         WHERE id IN (
           SELECT id FROM jobs
             WHERE status = 'pending' AND run_at <= now()
             ORDER BY run_at
             LIMIT $1
             FOR UPDATE SKIP LOCKED
         )
         RETURNING *`,
    [BATCH],
  );
  // FOR UPDATE locks the rows this worker takes; SKIP LOCKED lets other workers step over them.
  return rows;
}

// ===========================================================================
//  2 — failures must retry with backoff, then dead-letter
// ---------------------------------------------------------------------------
//  The 'flaky' jobs throw ~70% of the time. Failed jobs go back to 'pending'
//  with exponential backoff and jitter, then to 'dead' once attempts run out.
// ===========================================================================
async function onFailure(job: Job, err: unknown): Promise<void> {
  const attempts = job.attempts + 1;
  if (attempts < job.max_attempts) {
    const backoffSec = Math.min(2 ** attempts, 60) + Math.random(); // exp + jitter, capped
    await pool.query(
      `UPDATE jobs SET status='pending', attempts=$2, last_error=$3,
           run_at = now() + ($4 * interval '1 second')
         WHERE id=$1`,
      [job.id, attempts, String(err), backoffSec],
    );
  } else {
    await pool.query(
      `UPDATE jobs SET status='dead', attempts=$2, last_error=$3 WHERE id=$1`,
      [job.id, attempts, String(err)],
    );
  }
}

// ===========================================================================
//  3 — reclaim jobs whose worker died
// ---------------------------------------------------------------------------
//  main() inserts one row already stuck in 'running' with an old locked_at
//  (a worker SIGKILLed mid-job). Nothing brings it back on its own.
// ===========================================================================
async function reaper(): Promise<void> {
  const { rowCount } = await pool.query(
    `UPDATE jobs SET status='pending', locked_at=NULL
         WHERE status='running'
           AND locked_at < now() - ($1 * interval '1 second')`,
    [VISIBILITY_TIMEOUT_SEC],
  );
  if (rowCount) console.log(`  reaper: reset ${rowCount} stuck job(s)`);
  //  This is the "visibility timeout" every queue has (SQS calls it exactly that).
}

async function runJob(job: Job): Promise<void> {
  const handler = handlers[job.type];
  if (!handler) {
    await onFailure(job, `no handler for type "${job.type}"`);
    return;
  }
  try {
    await handler(job.payload);
    await pool.query(`UPDATE jobs SET status = 'done' WHERE id = $1`, [job.id]);
  } catch (err) {
    await onFailure(job, err);
  }
}

async function worker(deadline: number): Promise<void> {
  while (Date.now() < deadline) {
    const jobs = await claimJobs();
    if (jobs.length === 0) {
      await sleep(100); // poll interval
      continue;
    }
    for (const job of jobs) await runJob(job);
  }
}

async function main(): Promise<void> {
  await pool.query(readFileSync(join(here, "schema.sql"), "utf8"));

  await enqueue("send_email", { to: "a@x.com" });
  await enqueue("send_email", { to: "b@x.com" });
  await enqueue(
    "generate_report",
    { month: "2026-09" },
    new Date(Date.now() + 2000),
  );
  for (let i = 0; i < 5; i++) await enqueue("flaky", { i });

  // an orphaned job: a worker claimed it, then was SIGKILLed
  await pool.query(
    `INSERT INTO jobs (type, payload, status, locked_at)
     VALUES ('send_email', '{"to":"orphan@x.com"}', 'running', now() - interval '2 minutes')`,
  );

  const deadline = Date.now() + 2 * 6000;
  const reaperLoop = (async () => {
    while (Date.now() < deadline) {
      await reaper();
      await sleep(1000);
    }
  })();

  await Promise.all([
    worker(deadline),
    worker(deadline),
    worker(deadline),
    reaperLoop,
  ]);

  const { rows } = await pool.query(
    `SELECT status, count(*)::int AS n FROM jobs GROUP BY status ORDER BY status`,
  );
  console.log("\nfinal job states:", rows);
  console.log("\nhandler call log:");
  for (const line of handlerLog) console.log("  " + line);

  await pool.end();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
