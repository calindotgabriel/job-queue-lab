// Fake job handlers. Every call appends to handlerLog so you can see exactly
// what ran, how many times, and what threw.

export const handlerLog: string[] = [];

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Handler = (payload: any) => Promise<void>;

export const handlers: Record<string, Handler> = {
  async send_email(p) {
    await wait(20);
    handlerLog.push(`send_email -> ${p.to}`);
  },

  async generate_report(p) {
    await wait(50);
    handlerLog.push(`generate_report -> ${p.month}`);
  },

  // throws ~70% of the time -> exercises retry / backoff / dead-letter
  async flaky(p) {
    await wait(10);
    if (Math.random() < 0.7) {
      handlerLog.push(`flaky #${p.i} THREW`);
      throw new Error("flaky failure");
    }
    handlerLog.push(`flaky #${p.i} ok`);
  },
};
