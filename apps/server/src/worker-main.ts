/**
 * Entry point of one worker PROCESS. The server spawns these as real child processes so that
 * "kill worker" is a genuine SIGKILL: no cleanup handlers, no graceful lease release.
 */
import { Store, Tracing, Worker } from '@durable-flow/engine';
import { buildRegistry, getSettings } from '@durable-flow/examples';

const id = process.env.WORKER_ID ?? `W${process.pid}`;
const url = process.env.DATABASE_URL!;
const leaseMs = Number(process.env.LEASE_MS ?? 4000);

const store = new Store(url, 8);
const worker = new Worker({
  id, store, registry: buildRegistry(), leaseMs, heartbeatMs: Math.max(200, Math.floor(leaseMs / 4)), pollMs: 150, maxConcurrent: 4,
  tracing: new Tracing(store.pool),
  log: (m) => console.log(m),
  hooks: {
    async beforeActivity(name, ac) {
      const s = await getSettings(store);
      // Chaos: inject a transient failure (never into compensations or retries beyond the first few attempts).
      if (s.failure_rate > 0 && ac.attempt < 3 && Math.random() < s.failure_rate) throw new Error(`injected transient failure in ${name}`);
    },
    async afterActivityBeforeRecord(name) {
      // "Arm crash": die at the worst possible moment - effect applied, completion not yet recorded.
      const s = await getSettings(store);
      if (s.armed_crash === name && (await store.consumeSetting('armed_crash', name))) {
        console.log(`[${id}] *** armed crash: dying after "${name}" took effect but before it was recorded ***`);
        process.kill(process.pid, 'SIGKILL');
        await new Promise(() => {});
      }
    },
  },
});

await worker.start();
console.log(`[${id}] online (pid ${process.pid}, lease ${leaseMs}ms)`);

const shutdown = async () => { await worker.stop().catch(() => {}); process.exit(0); };
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
