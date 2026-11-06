import { describe, expect, it } from 'vitest';
import { fuzzReplay, HistoryEvent, replay, WorkflowFn } from '../src/index.js';

let seq = 0;
const ev = (type: string, payload: any = {}): HistoryEvent => ({ seq: ++seq, type: type as any, payload, ts: new Date().toISOString() });
const started = (input: any = {}) => { seq = 0; return ev('WorkflowStarted', { input }); };

const wf: WorkflowFn = async (ctx, input) => {
  const a = await ctx.activity('first', { x: input.x });
  const b = await ctx.activity('second', a);
  await ctx.sleep(1000);
  return { a, b };
};

describe('deterministic replay', () => {
  it('emits the first command and parks on a fresh history', async () => {
    const r = await replay(wf, [started({ x: 1 })], { workflowId: 'w' });
    expect(r.done).toBe(false);
    expect(r.newEvents.map((e) => e.type)).toEqual(['ActivityScheduled']);
    expect(r.pending).toHaveLength(1);
  });

  it('resumes from history: completed commands return recorded results, next command is new', async () => {
    const h = [started({ x: 1 }), ev('ActivityScheduled', { cmd: 1, name: 'first', input: { x: 1 }, retry: {} }),
      ev('ActivityStarted', { cmd: 1, attempt: 1 }), ev('ActivityCompleted', { cmd: 1, attempt: 1, result: 42 })];
    const r = await replay(wf, h, { workflowId: 'w' });
    expect(r.newEvents).toHaveLength(1);
    expect(r.newEvents[0].payload).toMatchObject({ cmd: 2, name: 'second', input: 42 });
  });

  it('replaying the same history twice yields identical output (determinism)', async () => {
    const h = [started({ x: 1 }),
      ev('ActivityScheduled', { cmd: 1, name: 'first', input: { x: 1 }, retry: {} }), ev('ActivityCompleted', { cmd: 1, attempt: 1, result: 'A' }),
      ev('ActivityScheduled', { cmd: 2, name: 'second', input: 'A', retry: {} }), ev('ActivityCompleted', { cmd: 2, attempt: 1, result: 'B' }),
      ev('TimerStarted', { cmd: 3, ms: 1000, fireAt: new Date().toISOString() }), ev('TimerFired', { cmd: 3 })];
    const r1 = await replay(wf, h, { workflowId: 'w' });
    const r2 = await replay(wf, h, { workflowId: 'w' });
    expect(r1.done).toBe(true);
    expect(r1.output).toEqual({ a: 'A', b: 'B' });
    expect(r2.output).toEqual(r1.output);
    expect(r1.newEvents).toHaveLength(0);
  });

  it('records now()/random()/uuid() once and replays the same values', async () => {
    const fn: WorkflowFn = async (ctx) => ({ t: ctx.now(), r: ctx.random(), u: ctx.uuid() });
    const r1 = await replay(fn, [started()], { workflowId: 'w' });
    const h = [started(), ...r1.newEvents.map((e) => ev(e.type, e.payload))];
    const r2 = await replay(fn, h, { workflowId: 'w' });
    expect(r2.output).toEqual(r1.output);
    expect(r2.newEvents).toHaveLength(0);
  });

  it('detects non-determinism when code diverges from history', async () => {
    const h = [started({ x: 1 }), ev('ActivityScheduled', { cmd: 1, name: 'first', input: {}, retry: {} }), ev('ActivityCompleted', { cmd: 1, attempt: 1, result: 1 })];
    const changed: WorkflowFn = async (ctx) => { await ctx.activity('renamed', {}); };
    const r = await replay(changed, h, { workflowId: 'w' });
    expect(r.nonDeterminism).toMatch(/command #1/);
  });

  it('detects non-determinism even if workflow code swallows the error', async () => {
    const h = [started(), ev('ActivityScheduled', { cmd: 1, name: 'first', input: {}, retry: {} }), ev('ActivityCompleted', { cmd: 1, attempt: 1, result: 1 })];
    const sneaky: WorkflowFn = async (ctx) => { try { await ctx.activity('other', {}); } catch { /* swallow */ } return 'ok'; };
    expect((await replay(sneaky, h, { workflowId: 'w' })).nonDeterminism).toBeTruthy();
  });

  it('detects non-determinism when code finishes but history has more commands', async () => {
    const h = [started(), ev('ActivityScheduled', { cmd: 1, name: 'a', input: {}, retry: {} }), ev('ActivityCompleted', { cmd: 1, attempt: 1, result: 1 }),
      ev('ActivityScheduled', { cmd: 2, name: 'b', input: {}, retry: {} })];
    const shorter: WorkflowFn = async (ctx) => { await ctx.activity('a', {}); };
    expect((await replay(shorter, h, { workflowId: 'w' })).nonDeterminism).toMatch(/command #2/);
  });

  it('ctx.patched() lets new code run against in-flight old histories', async () => {
    // v1 code was: a -> b.   v2 code inserts an audit step between them, gated by patched().
    const v2: WorkflowFn = async (ctx) => {
      await ctx.activity('a', {});
      if (ctx.patched('add-audit')) await ctx.activity('audit', {});
      await ctx.activity('b', {});
      return 'done';
    };
    const inFlightOld = [started(), ev('ActivityScheduled', { cmd: 1, name: 'a', input: {}, retry: {} }), ev('ActivityCompleted', { cmd: 1, attempt: 1, result: 1 }),
      ev('ActivityScheduled', { cmd: 2, name: 'b', input: {}, retry: {} })];
    const old = await replay(v2, inFlightOld, { workflowId: 'w' });
    expect(old.nonDeterminism).toBeUndefined();          // old path preserved: cmd 2 is still "b"
    expect(old.pending[0]).toMatchObject({ kind: 'activity', name: 'b' });
    expect(old.newEvents).toHaveLength(0);
    // Without the gate the same history would be flagged:
    const ungated: WorkflowFn = async (ctx) => { await ctx.activity('a', {}); await ctx.activity('audit', {}); await ctx.activity('b', {}); };
    expect((await replay(ungated, inFlightOld, { workflowId: 'w' })).nonDeterminism).toMatch(/command #2/);
    // A brand-new execution takes the patched path and records a marker.
    const fresh = await replay(v2, [started(), ev('ActivityScheduled', { cmd: 1, name: 'a', input: {}, retry: {} }), ev('ActivityCompleted', { cmd: 1, attempt: 1, result: 1 })], { workflowId: 'w' });
    expect(fresh.newEvents.map((e) => e.type)).toEqual(['PatchMarker', 'ActivityScheduled']);
    expect(fresh.newEvents[1].payload.name).toBe('audit');
  });

  it('supports Promise.all / race with deterministic command numbering', async () => {
    const fn: WorkflowFn = async (ctx) => {
      const [a, b] = await Promise.all([ctx.activity('a', 1), ctx.activity('b', 2)]);
      return a + b;
    };
    const r = await replay(fn, [started()], { workflowId: 'w' });
    expect(r.newEvents.map((e) => e.payload.cmd)).toEqual([1, 2]);
    const h = [started(), ev('ActivityScheduled', { cmd: 1, name: 'a', input: 1, retry: {} }), ev('ActivityScheduled', { cmd: 2, name: 'b', input: 2, retry: {} }),
      ev('ActivityCompleted', { cmd: 2, attempt: 1, result: 20 }), ev('ActivityCompleted', { cmd: 1, attempt: 1, result: 10 })];
    expect((await replay(fn, h, { workflowId: 'w' })).output).toBe(30);
  });
});

describe('determinism fuzzer', () => {
  const good: WorkflowFn = async (ctx) => { const t = ctx.now(); await ctx.activity('a', t); return 1; };
  const bad: WorkflowFn = async (ctx) => { if (Date.now() % 2 === 0) await ctx.activity('even', 1); else await ctx.activity('odd', 1); };

  it('passes workflows that only use ctx.*', async () => {
    const rep = await fuzzReplay(good, [started()], 10);
    expect(rep.deterministic).toBe(true);
  });
  it('flags workflows that read the real clock', async () => {
    const rep = await fuzzReplay(bad, [started()], 20);
    expect(rep.deterministic).toBe(false);
    expect(rep.violations.some((v) => v.api === 'Date.now()')).toBe(true);
  });
});

describe('queries', () => {
  it('exposes live workflow state through query handlers, at any point in history', async () => {
    const fn: WorkflowFn = async (ctx) => {
      const state = { step: 'start' };
      ctx.setQueryHandler('status', () => state);
      state.step = 'charging';
      await ctx.activity('charge');
      state.step = 'done';
    };
    const parked = await replay(fn, [started()], { workflowId: 'w' });
    expect(parked.queryHandlers.get('status')!()).toEqual({ step: 'charging' });
    const h = [started(), ev('ActivityScheduled', { cmd: 1, name: 'charge', input: null, retry: {} }), ev('ActivityCompleted', { cmd: 1, attempt: 1, result: 1 })];
    const finished = await replay(fn, h, { workflowId: 'w' });
    expect(finished.queryHandlers.get('status')!()).toEqual({ step: 'done' });
  });
});
