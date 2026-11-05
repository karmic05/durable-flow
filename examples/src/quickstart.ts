/**
 * Quickstart: a workflow that survives a crash.   Run:  npm run quickstart
 *
 *  1. starts a throwaway Postgres (no Docker needed)
 *  2. defines a 3-step workflow and its activities
 *  3. starts Worker A, which "dies" right after charging the card but before recording it
 *  4. starts Worker B, which resumes from the history
 *  5. prints the event history and proves the card was charged exactly once
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { Registry, startEmbeddedPostgres, Store, Worker } from '@durable-flow/engine';

// ── 1. A database. Swap this for `new Store(process.env.DATABASE_URL!)` to use your own Postgres. ──
const pg = await startEmbeddedPostgres({ dir: fs.mkdtempSync(path.join(os.tmpdir(), 'df-quickstart-')), port: 54340, persistent: false });
const store = new Store(pg.url());
await store.migrate();

// ── 2. The outside world. Every call gets an idempotency key that is stable across retries and takeovers. ──
const charged = new Map<string, number>();          // key -> how many times the card was really charged
const chargeCard = async (key: string, amount: number) => {
  if (!charged.has(key)) charged.set(key, 0);
  if (charged.get(key)! === 0) { charged.set(key, 1); console.log(`   💳 charged $${amount} (key ${key})`); }
  else console.log(`   ↩︎  charge ignored, key ${key} already seen`);
};

// ── 3. Define the workflow: plain async code. ──
const registry = new Registry()
  .workflow('checkout', async (ctx, order: { id: string; amount: number }) => {
    await ctx.activity('reserveStock', order);
    await ctx.activity('chargeCard', order);
    await ctx.sleep(1000);                           // a durable timer: survives restarts
    await ctx.activity('sendReceipt', order);
    return `order ${order.id} complete`;
  })
  .activity('reserveStock', async (order) => { console.log(`   📦 reserved stock for ${order.id}`); })
  .activity('chargeCard', async (order, ac) => { await chargeCard(ac.idempotencyKey, order.amount); })
  .activity('sendReceipt', async (order) => { console.log(`   ✉️  receipt sent for ${order.id}`); });

// ── 4. Start the workflow, then run workers. ──
await store.createWorkflow({ id: 'order-1001', type: 'checkout', input: { id: '1001', amount: 49 } });

const opts = { store, registry, leaseMs: 1500, heartbeatMs: 300, pollMs: 50 };

console.log('\n▶ Worker A starts');
const A: Worker = new Worker({
  ...opts, id: 'A',
  hooks: {
    afterActivityBeforeRecord: async (name) => {
      if (name === 'chargeCard') {                   // simulate a hard crash in the worst possible spot
        console.log('   💥 Worker A killed: the card WAS charged, but the result was never recorded');
        A.crash();
        await new Promise(() => {});
      }
    },
  },
});
await A.start();
await new Promise((r) => setTimeout(r, 1800));       // A is dead; its lease will expire

console.log('\n▶ Worker B starts (A is dead, its lease expires, B takes over)');
const B = new Worker({ ...opts, id: 'B' });
await B.start();

// ── 5. Wait for completion and show what happened. ──
for (;;) {
  const wf = await store.getWorkflow('order-1001');
  if (wf && wf.status !== 'running') { console.log(`\n✔ ${wf.status}: ${JSON.stringify(wf.result)}`); break; }
  await new Promise((r) => setTimeout(r, 100));
}

console.log('\nEvent history (what Postgres stored):');
for (const e of await store.getEvents('order-1001')) {
  const detail = e.type === 'WorkerTakeover' ? `${e.payload.from} → ${e.payload.to}` : (e.payload.name ?? (e.payload.cmd ? `#${e.payload.cmd}` : ''));
  console.log(`  ${String(e.seq).padStart(2)}  ${e.type.padEnd(18)} ${detail}`);
}
const times = [...charged.values()].reduce((a, b) => a + b, 0);
console.log(`\nCard charged ${times}× for 1 order.  ${times === 1 ? 'No duplicate side effects.' : 'DUPLICATE!'}`);

await A.stop(); await B.stop(); await store.close(); await pg.stop();
process.exit(times === 1 ? 0 : 1);
