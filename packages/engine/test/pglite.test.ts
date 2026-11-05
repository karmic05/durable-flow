import { PGlite } from '@electric-sql/pglite';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { PglitePool, Registry, StoreBase, Worker } from '../src/browser.js';

// Same engine, but the database is Postgres-in-WebAssembly (PGlite): exactly what runs in the website playground.
const toStop: Worker[] = [];
afterEach(async () => { await Promise.allSettled(toStop.splice(0).map((w) => w.stop())); });

async function boot() {
  const db = new PGlite();
  const store = new StoreBase(new PglitePool(db));
  await store.migrate();
  return { db, store };
}
const until = async <T>(fn: () => Promise<T | undefined | false>, ms = 20000): Promise<T> => {
  const end = Date.now() + ms;
  for (;;) { const v = await fn(); if (v) return v as T; if (Date.now() > end) throw new Error('timed out'); await new Promise((r) => setTimeout(r, 40)); }
};
const mk = (id: string, store: StoreBase, registry: Registry, extra: any = {}) =>
  new Worker({ id, store, registry, leaseMs: 800, heartbeatMs: 200, pollMs: 25, ...extra });

describe('engine on PGlite (Postgres in WebAssembly)', () => {
  it('runs a workflow with real SQL under the hood', async () => {
    const { store } = await boot();
    const reg = new Registry()
      .workflow('w', async (ctx, i) => { const a = await ctx.activity('inc', i.n); await ctx.sleep(150); return await ctx.activity('inc', a); })
      .activity('inc', async (n) => n + 1);
    await store.createWorkflow({ id: 'p1', type: 'w', input: { n: 1 } });
    const w = mk('A', store, reg); toStop.push(w); await w.start();
    const wf = await until(async () => { const x = await store.getWorkflow('p1'); return x && x.status !== 'running' ? x : undefined; });
    expect(wf.result).toBe(3);
    const rows = await store.pool.query('SELECT count(*)::int AS n FROM events WHERE workflow_id = $1', ['p1']);
    expect(rows.rows[0].n).toBeGreaterThan(8);
  });

  it('survives a crash after the effect but before recording, with no duplicate effect', async () => {
    const { store } = await boot();
    const reg = new Registry().workflow('o', async (ctx) => { await ctx.activity('reserve'); await ctx.activity('charge'); await ctx.activity('ship'); return 'ok'; });
    for (const n of ['reserve', 'charge', 'ship']) {
      reg.activity(n, async (_i, ac) => {
        await store.pool.query(`INSERT INTO side_effects (idempotency_key,service,action,protected) VALUES ($1,$2,'do',true) ON CONFLICT DO NOTHING`, [ac.idempotencyKey, n]);
      });
    }
    await store.createWorkflow({ id: 'c1', type: 'o', input: {} });
    let A!: Worker;
    A = mk('A', store, reg, { hooks: { afterActivityBeforeRecord: async (name: string) => { if (name === 'charge') { A.crash(); await new Promise(() => {}); } } } });
    toStop.push(A); await A.start();
    await until(async () => (await store.pool.query(`SELECT 1 FROM side_effects WHERE idempotency_key='c1:2'`)).rowCount);
    const B = mk('B', store, reg); toStop.push(B); await B.start();
    const wf = await until(async () => { const x = await store.getWorkflow('c1'); return x && x.status !== 'running' ? x : undefined; });
    expect(wf.status).toBe('completed');
    const ev = await store.getEvents('c1');
    expect(ev.some((e) => e.type === 'WorkerTakeover' && e.payload.from === 'A' && e.payload.to === 'B')).toBe(true);
    const eff = await store.pool.query('SELECT idempotency_key k, count(*)::int c FROM side_effects GROUP BY 1 ORDER BY 1');
    expect(eff.rows.map((r) => [r.k, r.c])).toEqual([['c1:1', 1], ['c1:2', 1], ['c1:3', 1]]);
  });

  it('signals, child workflows and sagas all work through the adapter', async () => {
    const { store } = await boot();
    const log: string[] = [];
    const reg = new Registry()
      .workflow('parent', async (ctx) => {
        const saga = ctx.saga();
        try {
          await ctx.activity('a'); saga.add('undoA');
          const who = await ctx.waitForSignal<string>('approve');
          const k = await ctx.child<number>('kid', { n: 4 });
          await ctx.activity('boom');
          return [who, k];
        } catch (e) { await saga.compensate(); throw e; }
      })
      .workflow('kid', async (ctx, i) => ctx.activity<number>('dbl', i.n))
      .activity('a', async () => { log.push('a'); })
      .activity('undoA', async () => { log.push('undoA'); })
      .activity('dbl', async (n) => n * 2)
      .activity('boom', async () => { throw Object.assign(new Error('nope'), { name: 'NonRetryableError' }); });
    await store.createWorkflow({ id: 'g1', type: 'parent', input: {} });
    const w = mk('A', store, reg); toStop.push(w); await w.start();
    await until(async () => (await store.getEvents('g1')).some((e) => e.type === 'SignalWaitStarted'));
    expect(await store.signal('g1', 'approve', 'sam')).toBe(true);
    const wf = await until(async () => { const x = await store.getWorkflow('g1'); return x && x.status !== 'running' ? x : undefined; });
    expect(wf.status).toBe('failed');
    expect(log).toEqual(['a', 'undoA']);
    expect((await store.getWorkflow('g1/4'))?.result).toBe(8);
  });

  it('works in a browser-like environment where setImmediate does not exist', async () => {
    vi.stubGlobal('setImmediate', undefined);
    try {
      expect(typeof setImmediate).toBe('undefined');
      const { store } = await boot();
      const reg = new Registry()
        .workflow('w', async (ctx) => { await ctx.activity('x'); await ctx.sleep(100); return 'browser ok'; })
        .activity('x', async () => 1);
      await store.createWorkflow({ id: 'b1', type: 'w', input: {} });
      const w = mk('A', store, reg); toStop.push(w); await w.start();
      const wf = await until(async () => { const x = await store.getWorkflow('b1'); return x && x.status !== 'running' ? x : undefined; });
      expect(wf.result).toBe('browser ok');
    } finally { vi.unstubAllGlobals(); }
  });
});
