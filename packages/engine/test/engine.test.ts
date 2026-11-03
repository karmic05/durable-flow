import { afterEach, describe, expect, it } from 'vitest';
import { LeaseLostError, NonRetryableError, Registry, Store, Worker } from '../src/index.js';
import { applyEffect, newStore, status, waitFor, worker } from './helpers.js';

const toStop: Worker[] = [];
afterEach(async () => { await Promise.allSettled(toStop.splice(0).map((w) => w.stop())); });
const run = async (w: Worker) => { toStop.push(w); await w.start(); return w; };

async function types(store: Store, id: string) { return (await store.getEvents(id)).map((e) => e.type); }

describe('engine end-to-end', () => {
  it('runs a multi-step workflow to completion', async () => {
    const store = await newStore();
    const reg = new Registry()
      .workflow('w', async (ctx, i) => { const a = await ctx.activity('inc', i.n); const b = await ctx.activity('inc', a); return b; })
      .activity('inc', async (n) => n + 1);
    await store.createWorkflow({ id: 'w1', type: 'w', input: { n: 1 } });
    await run(worker('A', store, reg));
    const wf = await waitFor(status(store, 'w1'));
    expect(wf.status).toBe('completed');
    expect(wf.result).toBe(3);
    await store.close();
  });

  it('survives a worker crash AFTER the side effect but BEFORE it was recorded, with no duplicate effects', async () => {
    const store = await newStore();
    const reg = new Registry()
      .workflow('order', async (ctx) => {
        await ctx.activity('reserve'); await ctx.activity('charge'); await ctx.activity('ship'); return 'done';
      });
    for (const n of ['reserve', 'charge', 'ship']) reg.activity(n, async (_i, ac) => { await applyEffect(store, ac.idempotencyKey, n); return n; });
    await store.createWorkflow({ id: 'o1', type: 'order', input: {} });

    let A!: Worker;
    // Worker A applies the "charge" effect and then dies before recording ActivityCompleted.
    A = worker('A', store, reg, { hooks: { afterActivityBeforeRecord: async (name) => { if (name === 'charge') { A.crash(); await new Promise(() => {}); } } } });
    await run(A);
    await waitFor(async () => (await types(store, 'o1')).includes('ActivityStarted') && (await store.getWorkflow('o1'))?.lease_owner === 'A' && (await store.pool.query(`select 1 from side_effects where idempotency_key='o1:2'`)).rowCount);

    await run(worker('B', store, reg));
    const wf = await waitFor(status(store, 'o1'), 20000);
    expect(wf.status).toBe('completed');

    const events = await store.getEvents('o1');
    expect(events.some((e) => e.type === 'WorkerTakeover' && e.payload.from === 'A' && e.payload.to === 'B')).toBe(true);
    const starts = events.filter((e) => e.type === 'ActivityStarted' && e.payload.cmd === 2);
    expect(starts).toHaveLength(2);                       // charge was executed twice...
    expect(starts[1].payload.orphan).toBe(true);
    const rows = await store.pool.query('select idempotency_key, count(*) c from side_effects group by 1 order by 1');
    expect(rows.rows.map((r) => [r.idempotency_key, Number(r.c)])).toEqual([['o1:1', 1], ['o1:2', 1], ['o1:3', 1]]); // ...but its effect happened once
    await store.close();
  });

  it('without idempotency protection the same crash DOES duplicate the effect (the demo contrast)', async () => {
    const store = await newStore();
    const reg = new Registry().workflow('o', async (ctx) => { await ctx.activity('charge'); return 1; });
    reg.activity('charge', async (_i, ac) => { await applyEffect(store, ac.idempotencyKey, 'charge', false); });
    await store.createWorkflow({ id: 'n1', type: 'o', input: {} });
    let A!: Worker;
    A = worker('A', store, reg, { hooks: { afterActivityBeforeRecord: async () => { A.crash(); await new Promise(() => {}); } } });
    await run(A);
    await waitFor(async () => (await store.pool.query(`select 1 from side_effects`)).rowCount);
    await run(worker('B', store, reg));
    await waitFor(status(store, 'n1'), 20000);
    const c = await store.pool.query(`select count(*) c from side_effects where idempotency_key='n1:1'`);
    expect(Number(c.rows[0].c)).toBe(2);
    await store.close();
  });

  it('fences a zombie worker: stale epoch cannot append', async () => {
    const store = await newStore();
    await store.createWorkflow({ id: 'z', type: 'w', input: {} });
    const c1 = (await store.claim('X', 5000))!;
    await store.pool.query(`update workflows set lease_expires_at = now() - interval '1 second' where id='z'`);
    const c2 = (await store.claim('Y', 5000))!;
    expect(c2.prevOwner).toBe('X');
    expect(c2.wf.lease_epoch).toBe(c1.wf.lease_epoch + 1);
    await expect(store.append('z', c1.wf.lease_epoch, [{ type: 'TimerFired', payload: { cmd: 99 } }])).rejects.toBeInstanceOf(LeaseLostError);
    await expect(store.append('z', c2.wf.lease_epoch, [{ type: 'TimerFired', payload: { cmd: 99 } }])).resolves.toHaveLength(1);
    await store.close();
  });

  it('two workers never run the same workflow concurrently (SKIP LOCKED)', async () => {
    const store = await newStore();
    await store.createWorkflow({ id: 'c', type: 'w', input: {} });
    const [a, b] = await Promise.all([store.claim('A', 5000), store.claim('B', 5000)]);
    expect([a, b].filter(Boolean)).toHaveLength(1);
    await store.close();
  });

  it('retries with backoff and records every attempt', async () => {
    const store = await newStore();
    const reg = new Registry()
      .workflow('r', async (ctx) => ctx.activity('flaky', null, { retry: { maxAttempts: 5, initialIntervalMs: 50, backoff: 1, jitter: 0 } }))
      .activity('flaky', async (_i, ac) => { if (ac.attempt < 3) throw new Error(`boom ${ac.attempt}`); return `ok@${ac.attempt}`; });
    await store.createWorkflow({ id: 'r1', type: 'r', input: {} });
    await run(worker('A', store, reg));
    const wf = await waitFor(status(store, 'r1'));
    expect(wf.result).toBe('ok@3');
    const fails = (await store.getEvents('r1')).filter((e) => e.type === 'ActivityFailed');
    expect(fails).toHaveLength(2);
    expect(fails.every((f) => f.payload.retryAt && !f.payload.final)).toBe(true);
    await store.close();
  });

  it('does not retry non-retryable errors and surfaces ActivityFailure to workflow code', async () => {
    const store = await newStore();
    const reg = new Registry()
      .workflow('r', async (ctx) => { try { await ctx.activity('bad'); return 'no'; } catch (e: any) { return e.name; } })
      .activity('bad', async () => { throw new NonRetryableError('card declined'); });
    await store.createWorkflow({ id: 'nr', type: 'r', input: {} });
    await run(worker('A', store, reg));
    const wf = await waitFor(status(store, 'nr'));
    expect(wf.result).toBe('ActivityFailure');
    expect((await types(store, 'nr')).filter((t) => t === 'ActivityStarted')).toHaveLength(1);
    await store.close();
  });

  it('durable timers: sleep survives with no worker holding the workflow', async () => {
    const store = await newStore();
    const reg = new Registry().workflow('t', async (ctx) => { await ctx.sleep(700); return 'woke'; });
    await store.createWorkflow({ id: 't1', type: 't', input: {} });
    const t0 = Date.now();
    const A = await run(worker('A', store, reg));
    await waitFor(async () => (await store.getWorkflow('t1'))?.lease_owner === null && (await types(store, 't1')).includes('TimerStarted'));
    A.crash();                                            // the only worker dies while the timer is pending
    await run(worker('B', store, reg));
    const wf = await waitFor(status(store, 't1'));
    expect(wf.result).toBe('woke');
    expect(Date.now() - t0).toBeGreaterThanOrEqual(650);
    await store.close();
  });

  it('saga: compensates completed steps in reverse order when a later step fails', async () => {
    const store = await newStore();
    const log: string[] = [];
    const reg = new Registry()
      .workflow('s', async (ctx) => {
        const saga = ctx.saga();
        try {
          await ctx.activity('reserve'); saga.add('unreserve', 'r');
          await ctx.activity('charge'); saga.add('refund', 'c');
          await ctx.activity('ship');   // fails
        } catch (e) { await saga.compensate(); throw e; }
      })
      .activity('reserve', async () => { log.push('reserve'); })
      .activity('charge', async () => { log.push('charge'); })
      .activity('ship', async () => { throw new NonRetryableError('no carrier'); })
      .activity('unreserve', async () => { log.push('unreserve'); })
      .activity('refund', async () => { log.push('refund'); });
    await store.createWorkflow({ id: 's1', type: 's', input: {} });
    await run(worker('A', store, reg));
    const wf = await waitFor(status(store, 's1'));
    expect(wf.status).toBe('failed');
    expect(log).toEqual(['reserve', 'charge', 'refund', 'unreserve']);
    const t = await types(store, 's1');
    expect(t).toContain('CompensationStarted'); expect(t).toContain('CompensationCompleted');
    await store.close();
  });

  it('signals wake a parked workflow; signals sent early are buffered', async () => {
    const store = await newStore();
    const reg = new Registry().workflow('sig', async (ctx) => {
      const a = await ctx.waitForSignal<string>('approve');
      const b = await ctx.waitForSignal<string>('approve');
      return [a, b];
    });
    await store.createWorkflow({ id: 'g1', type: 'sig', input: {} });
    await run(worker('A', store, reg));
    await waitFor(async () => (await types(store, 'g1')).includes('SignalWaitStarted'));
    expect(await store.signal('g1', 'approve', 'one')).toBe(true);
    expect(await store.signal('g1', 'approve', 'two')).toBe(true);
    const wf = await waitFor(status(store, 'g1'));
    expect(wf.result).toEqual(['one', 'two']);
    await store.close();
  });

  it('child workflows return results to the parent (and survive via the same machinery)', async () => {
    const store = await newStore();
    const reg = new Registry()
      .workflow('parent', async (ctx) => { const r = await ctx.child<number>('kid', { n: 20 }); return r + 1; })
      .workflow('kid', async (ctx, i) => (await ctx.activity<number>('dbl', i.n)))
      .activity('dbl', async (n) => n * 2);
    await store.createWorkflow({ id: 'p1', type: 'parent', input: {} });
    await run(worker('A', store, reg));
    const wf = await waitFor(status(store, 'p1'));
    expect(wf.result).toBe(41);
    expect((await store.getWorkflow('p1/1'))?.status).toBe('completed');
    await store.close();
  });

  it('continueAsNew starts a successor with fresh history and the chain completes', async () => {
    const store = await newStore();
    const reg = new Registry()
      .workflow('loop', async (ctx, i) => {
        await ctx.activity('tick', i.n);
        if (i.n < 3) ctx.continueAsNew({ n: i.n + 1 });
        return `finished at ${i.n}`;
      })
      .activity('tick', async () => 1);
    await store.createWorkflow({ id: 'L', type: 'loop', input: { n: 1 } });
    await run(worker('A', store, reg));
    const last = await waitFor(async () => { const w = await store.getWorkflow('L~2'); return w?.status === 'completed' ? w : undefined; });
    expect(last.result).toBe('finished at 3');
    expect((await store.getWorkflow('L'))?.status).toBe('completed');
    expect((await store.getEvents('L~2')).length).toBeLessThan(10);
    await store.close();
  });

  it('non-determinism parks the workflow as "stuck" with a clear error, and unstick resumes it after a fix', async () => {
    const store = await newStore();
    const v1 = new Registry().workflow('d', async (ctx) => { await ctx.activity('a'); await ctx.sleep(400); return 'v1'; }).activity('a', async () => 1).activity('b', async () => 2);
    await store.createWorkflow({ id: 'd1', type: 'd', input: {} });
    const A = await run(worker('A', store, v1));
    await waitFor(async () => (await types(store, 'd1')).includes('TimerStarted'));
    A.crash();
    // "Deploy" incompatible code: renames the first activity.
    const v2 = new Registry().workflow('d', async (ctx) => { await ctx.activity('b'); return 'v2'; }).activity('a', async () => 1).activity('b', async () => 2);
    const B = await run(worker('B', store, v2));
    const stuck = await waitFor(async () => { const w = await store.getWorkflow('d1'); return w?.status === 'stuck' ? w : undefined; });
    expect(stuck.error?.name).toBe('NonDeterminismError');
    // Roll back to compatible code and unstick.
    B.crash();
    await run(worker('C', store, v1));
    await store.unstick('d1');
    const done = await waitFor(status(store, 'd1'));
    expect(done.result).toBe('v1');
    await store.close();
  });
});
