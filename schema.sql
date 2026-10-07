-- Loaded by after.ts on every run, so each run starts clean.
DROP TABLE IF EXISTS jobs;

CREATE TABLE jobs (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  type         text NOT NULL,                       -- which handler runs it
  payload      jsonb NOT NULL DEFAULT '{}',
  run_at       timestamptz NOT NULL DEFAULT now(),  -- earliest time it may run (the "delay")
  status       text NOT NULL DEFAULT 'pending',     -- pending | running | done | failed | dead
  attempts     int  NOT NULL DEFAULT 0,
  max_attempts int  NOT NULL DEFAULT 3,
  locked_at    timestamptz,                         -- when a worker claimed it (for the reaper)
  last_error   text,
  created_at   timestamptz NOT NULL DEFAULT now()
);

-- the index the claim query rides: find due pending jobs, oldest first
CREATE INDEX jobs_claim_idx ON jobs (status, run_at);
