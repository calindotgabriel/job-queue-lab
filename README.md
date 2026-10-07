# job-queue-lab

"Send this email in 2 hours." An in-memory `setTimeout` loses the job when the process dies.
A Postgres table with a `run_at` column does not. This repo has both, and shows the three
things that go wrong once several workers poll the same table.

This is the measured half of **[calingabriel.com/lab/job-queue](https://calingabriel.com/lab/job-queue)**.
The page models the same race in your browser; the numbers it quotes come from running this against Postgres 16.

```
docker compose up -d     # Postgres 16 on localhost:5434, user postgres, password lab
npm install
npm run before           # job "c" never runs, the process crashes first
npm run after            # 3 workers and a reaper for about 6 seconds, then the final job states
```

## What is in it

- `before.ts`: a scheduler built on `setTimeout`. State dies with the process, and delays over about 24.8 days fire immediately.
- `after.ts`: a durable queue. One row per job, three workers polling.
- `schema.sql`, `handlers.ts`: the table and the fake handlers (email, report, and a flaky job that throws about 70% of the time).

## The three fixes in `after.ts`

1. **Claim in one statement.** `SELECT` then `UPDATE` lets two workers read the same rows, so jobs run 2 to 3 times. `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING *` locks the rows it takes and lets other workers step over them.
2. **Retry with backoff, then dead-letter.** A failed job goes back to `pending` with an exponential, jittered, capped delay. After `max_attempts` it becomes `dead`.
3. **Reaper.** A worker killed mid-job leaves a row stuck in `running`. A loop resets rows whose `locked_at` is older than the visibility timeout.

## The catch

This is at-least-once, not exactly-once. If a worker crashes after the side effect but before it marks the job `done`, the job runs again. Handlers have to be idempotent. That is the next lab.

MIT licensed.
