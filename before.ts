// BEFORE — an in-memory scheduler. Everything a durable queue has to fix.
//
//   npm run before
//
// Watch: job "c" (the delayed report) never runs. The process "crashes"
// one second in, and setTimeout state lives only in RAM.

interface Job {
  id: string;
  type: string;
  runAt: number;
}

const timers = new Map<string, NodeJS.Timeout>();

function schedule(job: Job, run: (j: Job) => void): void {
  const delay = job.runAt - Date.now();
  // FLAW 4: setTimeout clamps a delay > 2^31-1 ms (~24.8 days) and fires it
  //         almost immediately. "run in 60 days" would run right now.
  timers.set(
    job.id,
    setTimeout(() => run(job), Math.max(0, delay)),
  );
}

function run(job: Job): void {
  console.log(`  ran ${job.type} (${job.id}) at ${new Date().toISOString()}`);
}

console.log("scheduling 3 jobs...");
schedule({ id: "a", type: "send_email", runAt: Date.now() + 200 }, run);
schedule({ id: "b", type: "send_email", runAt: Date.now() + 400 }, run);
schedule({ id: "c", type: "generate_report", runAt: Date.now() + 3000 }, run);

setTimeout(() => {
  console.log("\n process crash (deploy / OOM / SIGKILL)\n");
  process.exit(1);
}, 1000);

// FLAWS — the "after" lab fixes each one:
//  1. Durability   — after the crash, job "c" is gone. No record it existed.
//  2. One process  — no way to add workers; a slow job blocks every pending callback.
//  3. No retries   — if run() throws, the job is simply lost.
//  4. Delay ceiling — setTimeout maxes at ~24.8 days; longer delays fire immediately.
//  5. No visibility — cannot query "what is pending / running / failed".
