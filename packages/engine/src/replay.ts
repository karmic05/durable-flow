import {
  ActivityFailure, ActivityOptions, ChildWorkflowFailure, ContinueAsNewSignal, DEFAULT_RETRY, EventDraft,
  HistoryEvent, NonDeterminismError, RetryPolicy, SerializedError,
} from './types.js';

// Captured at import so engine internals never go through (possibly trapped) globals.
const realNow = Date.now.bind(Date);
const realRandom = Math.random.bind(Math);
const randomUUID = () => globalThis.crypto.randomUUID();          // Node 20+ and every modern browser
const fileUrlToPath = (u: string) => {
  const p = decodeURIComponent(new URL(u).pathname);
  return /^\/[A-Za-z]:/.test(p) ? p.slice(1) : p;                  // file:///C:/x -> C:/x
};

/**
 * Yield to the event loop for one full macrotask, so every already-resolved promise has drained.
 * `setImmediate` is Node-only, so browsers fall back to a MessageChannel (also a real macrotask).
 */
const macrotask = (): Promise<void> =>
  typeof setImmediate === 'function'
    ? new Promise<void>((r) => setImmediate(r))
    : new Promise<void>((r) => { const c = new MessageChannel(); c.port1.onmessage = () => { c.port1.close(); r(); }; c.port2.postMessage(0); });

export type Kind = 'activity' | 'timer' | 'signal' | 'child' | 'value' | 'marker';

export interface Saga {
  /** Register a compensating activity. Runs (in reverse order) on compensate(). */
  add(activity: string, input?: any, opts?: ActivityOptions): void;
  compensate(): Promise<void>;
}

export interface WorkflowContext {
  readonly workflowId: string;
  activity<T = any>(name: string, input?: any, opts?: ActivityOptions): Promise<T>;
  sleep(ms: number): Promise<void>;
  waitForSignal<T = any>(name: string): Promise<T>;
  child<T = any>(type: string, input?: any): Promise<T>;
  now(): number;
  random(): number;
  uuid(): string;
  saga(): Saga;
  /** Versioning gate: true for new executions, false for histories recorded before this code existed. */
  patched(id: string): boolean;
  continueAsNew(input?: any): never;
  setQueryHandler(name: string, fn: () => any): void;
}

export type WorkflowFn = (ctx: WorkflowContext, input: any) => Promise<any>;

export interface CommandInfo {
  cmd: number;
  kind: Kind;
  name: string;
  input?: any;
  status: 'pending' | 'running' | 'retrying' | 'completed' | 'failed';
  result?: any;
  error?: SerializedError;
  attempts: number;
  compensation?: boolean;
  scheduledSeq?: number;
  resolvedSeq?: number;
  site?: { file: string; line: number };
}

export type PendingCommand =
  | { kind: 'activity'; cmd: number; name: string; input: any; retry: RetryPolicy; timeoutMs?: number; attempt: number;
      retryAt?: string; orphan: boolean; compensation: boolean }
  | { kind: 'timer'; cmd: number; fireAt: string }
  | { kind: 'signal'; cmd: number; name: string }
  | { kind: 'child'; cmd: number; childId: string; type: string; input: any };

export interface SagaEntry { cmd: number; name: string; input: any; status: 'registered' | 'compensated' | 'compensation_failed' }

export interface ReplayResult {
  done: boolean;
  output?: any;
  error?: SerializedError;
  continueAsNew?: { input: any };
  nonDeterminism?: string;
  newEvents: EventDraft[];
  commands: CommandInfo[];
  pending: PendingCommand[];
  sagaStack: SagaEntry[];
  patches: string[];
  queryHandlers: Map<string, () => any>;
  nextCmd: number;
  eventsReplayed: number;
}

// ---------------------------------------------------------------------------------------------
// History indexing
// ---------------------------------------------------------------------------------------------
interface CmdHist {
  cmd: number; kind: Kind; name: string;
  scheduled: HistoryEvent;
  started: HistoryEvent[]; failed: HistoryEvent[];
  resolved?: HistoryEvent;
}

function kindAndName(e: HistoryEvent): { kind: Kind; name: string } | null {
  const p = e.payload;
  switch (e.type) {
    case 'ActivityScheduled': return { kind: 'activity', name: p.name };
    case 'TimerStarted': return { kind: 'timer', name: '' };
    case 'SignalWaitStarted': return { kind: 'signal', name: p.name };
    case 'ChildScheduled': return { kind: 'child', name: p.type };
    case 'ValueRecorded': return { kind: 'value', name: p.kind };
    case 'CompensationRegistered': return { kind: 'marker', name: `register:${p.name}` };
    case 'CompensationStarted': return { kind: 'marker', name: 'compensate-start' };
    case 'CompensationCompleted': return { kind: 'marker', name: 'compensate-end' };
    default: return null;
  }
}

function indexHistory(events: HistoryEvent[]) {
  const hist = new Map<number, CmdHist>();
  const patches = new Set<string>();
  const signals: HistoryEvent[] = [];
  let maxCmd = 0;
  for (const e of events) {
    if (e.type === 'PatchMarker') { patches.add(e.payload.id); continue; }
    if (e.type === 'SignalReceived') signals.push(e);
    const cmd = e.payload?.cmd;
    if (typeof cmd !== 'number') continue;
    const kn = kindAndName(e);
    if (kn) {
      hist.set(cmd, { cmd, ...kn, scheduled: e, started: [], failed: [] });
      maxCmd = Math.max(maxCmd, cmd);
      continue;
    }
    const h = hist.get(cmd);
    if (!h) continue;
    switch (e.type) {
      case 'ActivityStarted': h.started.push(e); break;
      case 'ActivityFailed': if (e.payload.final) h.resolved = e; else h.failed.push(e); break;
      case 'ActivityCompleted': case 'TimerFired': case 'SignalMatched': case 'ChildCompleted': case 'ChildFailed':
        h.resolved = e; break;
    }
  }
  return { hist, patches, maxCmd, signals };
}

function siteOf(): { file: string; line: number } | undefined {
  const lines = (new Error().stack ?? '').split('\n').slice(1);
  for (const l of lines) {
    if (/[\\/]engine[\\/]src[\\/]/.test(l) || l.includes('node:internal') || l.includes('node_modules')) continue;
    const m = l.match(/\(?((?:file:\/\/)?[^()\s]+?):(\d+):(\d+)\)?\s*$/);
    if (!m) continue;
    let file = m[1];
    if (file.startsWith('file://')) { try { file = fileUrlToPath(file); } catch { /* keep */ } }
    return { file, line: Number(m[2]) };
  }
  return undefined;
}

const never = <T>() => new Promise<T>(() => { /* resolves only when history gains a result and we replay again */ });

export interface ReplayOptions {
  workflowId: string;
  /** Capture the source line of each command (slower; for the inspector). */
  captureSites?: boolean;
}

export async function replay(fn: WorkflowFn, events: HistoryEvent[], opts: ReplayOptions): Promise<ReplayResult> {
  const { hist, patches: histPatches, maxCmd } = indexHistory(events);
  const started = events.find((e) => e.type === 'WorkflowStarted');
  const input = started?.payload.input ?? null;

  let counter = 0;
  const newEvents: EventDraft[] = [];
  const commands: CommandInfo[] = [];
  const pending: PendingCommand[] = [];
  const sagaStack: SagaEntry[] = [];
  const queryHandlers = new Map<string, () => any>();
  const patches = new Set(histPatches);
  let nonDet: string | undefined;

  function fail(msg: string): never {
    nonDet ??= msg;
    throw new NonDeterminismError(msg);
  }

  function next(kind: Kind, name: string): { cmd: number; h?: CmdHist } {
    const cmd = ++counter;
    const h = hist.get(cmd);
    if (h && (h.kind !== kind || h.name !== name)) {
      fail(`command #${cmd}: workflow code issued ${kind} "${name}" but history recorded ${h.kind} "${h.name}". ` +
        `The workflow code changed since this execution started — gate the change with ctx.patched("<id>").`);
    }
    return { cmd, h };
  }
  function info(c: CommandInfo): CommandInfo {
    if (opts.captureSites) c.site = siteOf();
    commands.push(c);
    return c;
  }

  const ctx: WorkflowContext = {
    workflowId: opts.workflowId,

    activity(name, actInput, aopts) {
      const comp = !!(aopts as any)?._compensation;
      const { cmd, h } = next('activity', name);
      if (!h) {
        const retry: RetryPolicy = { ...DEFAULT_RETRY, ...(aopts?.retry ?? {}) };
        newEvents.push({ type: 'ActivityScheduled', payload: { cmd, name, input: actInput ?? null, retry, timeoutMs: aopts?.timeoutMs ?? null, compensation: comp } });
        info({ cmd, kind: 'activity', name, input: actInput, status: 'pending', attempts: 0, compensation: comp });
        pending.push({ kind: 'activity', cmd, name, input: actInput ?? null, retry, timeoutMs: aopts?.timeoutMs, attempt: 1, orphan: false, compensation: comp });
        return never();
      }
      const sp = h.scheduled.payload;
      const attempts = h.started.length ? Math.max(...h.started.map((s) => s.payload.attempt)) : 0;
      const ci: CommandInfo = info({
        cmd, kind: 'activity', name, input: sp.input, status: 'pending', attempts, compensation: !!sp.compensation,
        scheduledSeq: h.scheduled.seq,
      });
      if (h.resolved) {
        ci.resolvedSeq = h.resolved.seq;
        if (h.resolved.type === 'ActivityCompleted') {
          ci.status = 'completed'; ci.result = h.resolved.payload.result;
          return Promise.resolve(h.resolved.payload.result);
        }
        ci.status = 'failed'; ci.error = h.resolved.payload.error;
        return Promise.reject(new ActivityFailure(name, h.resolved.payload.attempt ?? attempts, h.resolved.payload.error));
      }
      const lastFail = h.failed[h.failed.length - 1];
      const lastStart = h.started[h.started.length - 1];
      const waitingRetry = lastFail && (!lastStart || lastStart.seq < lastFail.seq);
      ci.status = waitingRetry ? 'retrying' : lastStart ? 'running' : 'pending';
      pending.push({
        kind: 'activity', cmd, name, input: sp.input, retry: sp.retry, timeoutMs: sp.timeoutMs ?? undefined,
        attempt: h.failed.length + 1, retryAt: waitingRetry ? lastFail.payload.retryAt : undefined,
        orphan: !!lastStart && !waitingRetry, compensation: !!sp.compensation,
      });
      return never();
    },

    sleep(ms) {
      const { cmd, h } = next('timer', '');
      if (!h) {
        const fireAt = new Date(realNow() + ms).toISOString();
        newEvents.push({ type: 'TimerStarted', payload: { cmd, ms, fireAt } });
        info({ cmd, kind: 'timer', name: `sleep ${ms}ms`, input: { ms, fireAt }, status: 'pending', attempts: 0 });
        pending.push({ kind: 'timer', cmd, fireAt });
        return never();
      }
      const ci = info({ cmd, kind: 'timer', name: `sleep ${h.scheduled.payload.ms}ms`, input: { ms: h.scheduled.payload.ms, fireAt: h.scheduled.payload.fireAt },
        status: 'pending', attempts: 0, scheduledSeq: h.scheduled.seq });
      if (h.resolved) { ci.status = 'completed'; ci.resolvedSeq = h.resolved.seq; return Promise.resolve(); }
      pending.push({ kind: 'timer', cmd, fireAt: h.scheduled.payload.fireAt });
      return never();
    },

    waitForSignal(name) {
      const { cmd, h } = next('signal', name);
      if (!h) {
        newEvents.push({ type: 'SignalWaitStarted', payload: { cmd, name } });
        info({ cmd, kind: 'signal', name, status: 'pending', attempts: 0 });
        pending.push({ kind: 'signal', cmd, name });
        return never();
      }
      const ci = info({ cmd, kind: 'signal', name, status: 'pending', attempts: 0, scheduledSeq: h.scheduled.seq });
      if (h.resolved) {
        ci.status = 'completed'; ci.resolvedSeq = h.resolved.seq; ci.result = h.resolved.payload.payload;
        return Promise.resolve(h.resolved.payload.payload);
      }
      pending.push({ kind: 'signal', cmd, name });
      return never();
    },

    child(type, childInput) {
      const { cmd, h } = next('child', type);
      const childId = `${opts.workflowId}/${cmd}`;
      if (!h) {
        newEvents.push({ type: 'ChildScheduled', payload: { cmd, childId, type, input: childInput ?? null } });
        info({ cmd, kind: 'child', name: type, input: childInput, status: 'pending', attempts: 0 });
        pending.push({ kind: 'child', cmd, childId, type, input: childInput ?? null });
        return never();
      }
      const ci = info({ cmd, kind: 'child', name: type, input: h.scheduled.payload.input, status: 'pending', attempts: 0, scheduledSeq: h.scheduled.seq });
      if (h.resolved) {
        ci.resolvedSeq = h.resolved.seq;
        if (h.resolved.type === 'ChildCompleted') { ci.status = 'completed'; ci.result = h.resolved.payload.result; return Promise.resolve(h.resolved.payload.result); }
        ci.status = 'failed'; ci.error = h.resolved.payload.error;
        return Promise.reject(new ChildWorkflowFailure(type, h.resolved.payload.error));
      }
      pending.push({ kind: 'child', cmd, childId, type, input: h.scheduled.payload.input });
      return never();
    },

    now: () => value('now', () => realNow()),
    random: () => value('random', () => realRandom()),
    uuid: () => value('uuid', () => randomUUID()),

    saga() {
      return {
        add(activity, sagaInput, aopts) {
          const { cmd, h } = next('marker', `register:${activity}`);
          if (!h) newEvents.push({ type: 'CompensationRegistered', payload: { cmd, name: activity, input: sagaInput ?? null, opts: aopts ?? null } });
          info({ cmd, kind: 'marker', name: `register compensation: ${activity}`, input: sagaInput, status: 'completed', attempts: 0, scheduledSeq: h?.scheduled.seq });
          sagaStack.push({ cmd, name: activity, input: sagaInput ?? null, status: 'registered' });
          sagaOpts.set(cmd, aopts);
        },
        async compensate() {
          const todo = sagaStack.filter((s) => s.status === 'registered').reverse();
          const start = next('marker', 'compensate-start');
          if (!start.h) newEvents.push({ type: 'CompensationStarted', payload: { cmd: start.cmd, count: todo.length } });
          info({ cmd: start.cmd, kind: 'marker', name: `compensating ${todo.length} step(s)`, status: 'completed', attempts: 0, scheduledSeq: start.h?.scheduled.seq });
          let failed = 0;
          for (const s of todo) {
            try {
              await ctx.activity(s.name, s.input, { ...(sagaOpts.get(s.cmd) ?? {}), _compensation: true } as any);
              s.status = 'compensated';
            } catch { s.status = 'compensation_failed'; failed++; }
          }
          const end = next('marker', 'compensate-end');
          if (!end.h) newEvents.push({ type: 'CompensationCompleted', payload: { cmd: end.cmd, failed } });
          info({ cmd: end.cmd, kind: 'marker', name: failed ? `compensation finished (${failed} failed)` : 'compensation finished', status: 'completed', attempts: 0, scheduledSeq: end.h?.scheduled.seq });
        },
      };
    },

    patched(id) {
      if (patches.has(id)) return true;
      if (!hist.has(counter + 1)) {
        patches.add(id);
        newEvents.push({ type: 'PatchMarker', payload: { id } });
        return true;
      }
      return false;
    },

    continueAsNew(inp) { throw new ContinueAsNewSignal(inp ?? null); },
    setQueryHandler(name, f) { queryHandlers.set(name, f); },
  };
  const sagaOpts = new Map<number, ActivityOptions | undefined>();

  function value<T>(kind: 'now' | 'random' | 'uuid', gen: () => T): T {
    const { cmd, h } = next('value', kind);
    if (h) {
      info({ cmd, kind: 'value', name: kind, status: 'completed', attempts: 0, result: h.scheduled.payload.value, scheduledSeq: h.scheduled.seq });
      return h.scheduled.payload.value as T;
    }
    const v = gen();
    newEvents.push({ type: 'ValueRecorded', payload: { cmd, kind, value: v } });
    info({ cmd, kind: 'value', name: kind, status: 'completed', attempts: 0, result: v });
    return v;
  }

  // ---- run until the workflow function blocks on something unresolved ----
  let settled = false; let output: any; let error: any;
  void (async () => fn(ctx, input))().then(
    (v) => { settled = true; output = v; },
    (e) => { settled = true; error = e; });
  // All replayed results resolve as microtasks, so once the macrotask queue turns the function is either done or parked.
  await macrotask();

  const base = {
    newEvents, commands, pending, queryHandlers, nextCmd: counter + 1, eventsReplayed: events.length,
    sagaStack, patches: [...patches],
  };
  if (nonDet) return { ...base, done: false, nonDeterminism: nonDet, pending: [] };
  if (!settled) return { ...base, done: false };
  if (error instanceof ContinueAsNewSignal) return { ...base, done: true, continueAsNew: { input: error.input }, pending: [] };
  if (maxCmd > counter) {
    return { ...base, done: false, pending: [],
      nonDeterminism: `workflow code finished after ${counter} command(s) but history contains command #${maxCmd}. The workflow code changed since this execution started.` };
  }
  if (error) {
    return { ...base, done: true, pending: [], error: { name: error.name ?? 'Error', message: String(error.message ?? error), stack: error.stack } };
  }
  return { ...base, done: true, pending: [], output };
}

export function defaultOutput(r: ReplayResult) { return r.output ?? null; }
