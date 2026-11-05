import { Registry, ActivityContext, WorkflowDefinition } from './registry.js';
import { replay, PendingCommand } from './replay.js';
import type { StoreBase } from './store-base.js';
import type { Tracing, ActiveSpan } from './tracing.js';
import { EventDraft, HistoryEvent, LeaseLostError, NonRetryableError, SerializedError, WorkflowRow } from './types.js';

export interface WorkerHooks {
  /** Runs before an activity handler. May throw to inject a failure. */
  beforeActivity?(name: string, ac: ActivityContext): Promise<void> | void;
  /** Runs AFTER the side effect happened but BEFORE the result is recorded: the crash window that matters. */
  afterActivityBeforeRecord?(name: string, ac: ActivityContext): Promise<void> | void;
}

export interface WorkerOptions {
  id: string;
  store: StoreBase;
  registry: Registry;
  deps?: any;
  leaseMs?: number;
  heartbeatMs?: number;
  pollMs?: number;
  maxConcurrent?: number;
  tracing?: Tracing;
  hooks?: WorkerHooks;
  log?: (msg: string) => void;
}

const PID = typeof process !== 'undefined' ? process.pid : 0;   // no `process` in browsers
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const serializeError = (e: any): SerializedError => ({ name: e?.name ?? 'Error', message: String(e?.message ?? e), stack: e?.stack });

export class Worker {
  readonly id: string;
  private o: Required<Pick<WorkerOptions, 'leaseMs' | 'heartbeatMs' | 'pollMs' | 'maxConcurrent'>> & WorkerOptions;
  private running = false;
  private crashed = false;
  private active = new Map<string, Promise<void>>();
  private beat?: NodeJS.Timeout;

  constructor(opts: WorkerOptions) {
    this.id = opts.id;
    this.o = { leaseMs: 4000, heartbeatMs: 1000, pollMs: 150, maxConcurrent: 4, ...opts };
  }
  private log(m: string) { this.o.log?.(`[${this.id}] ${m}`); }
  get activeIds() { return [...this.active.keys()]; }

  async start() {
    this.running = true;
    await this.o.store.upsertWorker(this.id, PID, []);
    this.beat = setInterval(() => { this.o.store.upsertWorker(this.id, PID, this.activeIds).catch(() => {}); }, 500);
    void this.loop();
  }
  /** Simulate a hard kill in-process: stop heartbeating and writing, release nothing. (Real SIGKILL is tested via child processes.) */
  crash() { this.crashed = true; this.running = false; clearInterval(this.beat); }
  private guard() { if (this.crashed) throw new LeaseLostError('worker crashed'); }
  async stop() {
    this.running = false;
    clearInterval(this.beat);
    if (!this.crashed) await Promise.allSettled([...this.active.values()]);
    await this.o.store.markWorkerDead(this.id).catch(() => {});
  }

  private async loop() {
    while (this.running) {
      try {
        if (this.active.size < this.o.maxConcurrent) {
          const claim = await this.o.store.claim(this.id, this.o.leaseMs);
          if (claim) {
            const p = this.runWorkflow(claim.wf, claim.prevOwner)
              .catch((e) => { if (!(e instanceof LeaseLostError)) this.log(`workflow ${claim.wf.id} error: ${e?.stack ?? e}`); else this.log(`lost lease on ${claim.wf.id}`); })
              .finally(() => this.active.delete(claim.wf.id));
            this.active.set(claim.wf.id, p);
            continue;
          }
        }
      } catch (e) { this.log(`poll error: ${(e as Error).message}`); }
      await sleep(this.o.pollMs);
    }
  }

  // -------------------------------------------------------------------------------------------
  private async runWorkflow(wf: WorkflowRow, prevOwner: string | null) {
    const { store, registry, tracing } = this.o;
    const epoch = wf.lease_epoch;
    const def = registry.workflows.get(wf.type);
    if (!def) {
      this.log(`unknown workflow type ${wf.type}`);
      const evs = await store.getEvents(wf.id);
      await store.release(wf.id, epoch, evs[evs.length - 1].seq, new Date(Date.now() + 5000));
      return;
    }

    let lost = false;
    const hb = setInterval(() => {
      if (this.crashed) return;
      store.heartbeat(wf.id, epoch, this.id, this.o.leaseMs).then((ok) => { if (!ok) lost = true; }).catch(() => {});
    }, this.o.heartbeatMs);

    const parentCtx = tracing?.rootContext(wf.root_id);
    const task: ActiveSpan | undefined = tracing?.start('workflow.task', parentCtx!, {
      'workflow.id': wf.id, 'workflow.type': wf.type, 'worker.id': this.id, 'lease.epoch': epoch, 'takeover.from': prevOwner ?? '',
    });
    let taskError: string | undefined;

    try {
      if (prevOwner && prevOwner !== this.id) {
        this.log(`TAKEOVER ${wf.id} from ${prevOwner} (epoch ${epoch})`);
        await store.append(wf.id, epoch, [{ type: 'WorkerTakeover', payload: { from: prevOwner, to: this.id, epoch } }]);
      }

      let guard = 0;
      while (guard++ < 10_000) {
        this.guard();
        if (lost) throw new LeaseLostError();
        const events = await store.getEvents(wf.id);
        let lastSeq = events[events.length - 1].seq;

        const rs = tracing?.start('replay', task!.ctx, { 'workflow.id': wf.id, 'worker.id': this.id, 'events.replayed': events.length });
        const r = await replay(def.fn, events, { workflowId: wf.id });
        rs?.end({ attrs: { 'commands.new': r.newEvents.length } });

        if (r.nonDeterminism) {
          this.log(`NON-DETERMINISM in ${wf.id}: ${r.nonDeterminism}`);
          await store.finish(wf, epoch, {
            status: 'stuck', error: { name: 'NonDeterminismError', message: r.nonDeterminism },
            events: [{ type: 'WorkflowFailed', payload: { stuck: true, error: { name: 'NonDeterminismError', message: r.nonDeterminism } } }],
          });
          taskError = 'non-determinism';
          return;
        }

        const appendedNew = r.newEvents.length ? await store.append(wf.id, epoch, r.newEvents) : [];
        if (appendedNew.length) lastSeq = appendedNew[appendedNew.length - 1].seq;

        if (r.done) {
          if (r.continueAsNew) {
            const n = wf.id.includes('~') ? Number(wf.id.split('~').pop()) + 1 : 1;
            const newId = `${wf.id.split('~')[0]}~${n}`;
            await store.finish(wf, epoch, {
              status: 'completed', result: { continuedAsNew: newId },
              events: [{ type: 'WorkflowContinuedAsNew', payload: { newId, input: r.continueAsNew.input } }],
              continueAs: { id: newId, type: wf.type, input: r.continueAsNew.input, codeHash: def.hash },
            });
          } else if (r.error) {
            await store.finish(wf, epoch, { status: 'failed', error: r.error, events: [{ type: 'WorkflowFailed', payload: { error: r.error } }] });
          } else {
            await store.finish(wf, epoch, { status: 'completed', result: r.output ?? null, events: [{ type: 'WorkflowCompleted', payload: { result: r.output ?? null } }] });
          }
          return;
        }

        // ---- drive pending commands ----
        const merged = appendedNew.length ? [...events, ...appendedNew] : events;
        const drafts: EventDraft[] = [];
        const acts: Extract<PendingCommand, { kind: 'activity' }>[] = [];
        let wakeAt: number | null = null;
        const wake = (t: number) => { wakeAt = wakeAt === null ? t : Math.min(wakeAt, t); };
        const now = Date.now();
        const sigQueues = signalQueues(merged);

        for (const p of r.pending) {
          if (p.kind === 'timer') {
            const t = Date.parse(p.fireAt);
            if (t <= now) drafts.push({ type: 'TimerFired', payload: { cmd: p.cmd } }); else wake(t);
          } else if (p.kind === 'signal') {
            const q = sigQueues.get(p.name);
            if (q && q.length) { drafts.push({ type: 'SignalMatched', payload: { cmd: p.cmd, name: p.name, payload: q.shift()!.payload.payload } }); }
          } else if (p.kind === 'child') {
            await store.createWorkflow({ id: p.childId, type: p.type, input: p.input, parentId: wf.id, parentCmd: p.cmd, rootId: wf.root_id,
              codeHash: registry.workflows.get(p.type)?.hash });
          } else if (p.kind === 'activity') {
            if (p.retryAt && Date.parse(p.retryAt) > now) wake(Date.parse(p.retryAt)); else acts.push(p);
          }
        }

        let progressed = false;
        if (drafts.length) { await store.append(wf.id, epoch, drafts); progressed = true; }
        if (acts.length) {
          await Promise.all(acts.map((p) => this.runActivity(wf, epoch, p, task, () => lost)));
          progressed = true;
        }
        if (progressed) continue;
        if (appendedNew.length) continue; // new commands were just recorded; replay once more with them in history

        // Nothing more to do right now: park, to be woken by a timer, signal or child completion.
        await store.release(wf.id, epoch, lastSeq, wakeAt === null ? null : new Date(wakeAt));
        return;
      }
      throw new Error('workflow task exceeded iteration guard');
    } catch (e) {
      taskError = (e as Error).message;
      throw e;
    } finally {
      clearInterval(hb);
      task?.end({ error: taskError });
    }
  }

  // -------------------------------------------------------------------------------------------
  private async runActivity(wf: WorkflowRow, epoch: number, p: Extract<PendingCommand, { kind: 'activity' }>, task: ActiveSpan | undefined, isLost: () => boolean) {
    const { store, registry, hooks, tracing } = this.o;
    const key = `${wf.id}:${p.cmd}`;
    const ac: ActivityContext = { workflowId: wf.id, cmd: p.cmd, attempt: p.attempt, idempotencyKey: key, worker: this.id, store, deps: this.o.deps };
    this.guard();
    if (isLost()) throw new LeaseLostError();

    await store.append(wf.id, epoch, [{ type: 'ActivityStarted', payload: { cmd: p.cmd, attempt: p.attempt, worker: this.id, orphan: p.orphan } }]);
    const span = tracing?.start(`activity ${p.name}`, task!.ctx, {
      'workflow.id': wf.id, 'worker.id': this.id, 'activity.name': p.name, 'activity.attempt': p.attempt,
      'activity.idempotency_key': key, 'activity.resumed_after_crash': p.orphan, 'activity.compensation': p.compensation,
    });

    let result: any; let err: any;
    try {
      const handler = registry.activities.get(p.name);
      if (!handler) throw new NonRetryableError(`no activity registered with name "${p.name}"`);
      await hooks?.beforeActivity?.(p.name, ac);
      result = await withTimeout(handler(p.input, ac), p.timeoutMs ?? 30_000, p.name);
    } catch (e) { err = e; }

    if (err === undefined) {
      // <-- the dangerous window: effect applied, completion not yet durable. A crash here is recovered by idempotency keys.
      await hooks?.afterActivityBeforeRecord?.(p.name, ac);
      this.guard();
      await store.append(wf.id, epoch, [{ type: 'ActivityCompleted', payload: { cmd: p.cmd, attempt: p.attempt, result: result ?? null, worker: this.id } }]);
      span?.end();
      return;
    }
    this.guard();
    if (err instanceof LeaseLostError) { span?.end({ error: 'lease lost' }); throw err; }
    const se = serializeError(err);
    const nonRetryable = err instanceof NonRetryableError || p.retry.nonRetryable.includes(se.name);
    const final = nonRetryable || p.attempt >= p.retry.maxAttempts;
    const retryAt = final ? undefined : new Date(Date.now() + backoff(p.retry, p.attempt)).toISOString();
    await store.append(wf.id, epoch, [{ type: 'ActivityFailed', payload: { cmd: p.cmd, attempt: p.attempt, error: { name: se.name, message: se.message }, final, retryAt, worker: this.id } }]);
    span?.end({ error: se.message, attrs: { 'activity.final': final } });
  }
}

function backoff(r: { initialIntervalMs: number; backoff: number; maxIntervalMs: number; jitter: number }, attempt: number) {
  const base = Math.min(r.maxIntervalMs, r.initialIntervalMs * Math.pow(r.backoff, attempt - 1));
  return Math.max(0, Math.round(base * (1 + r.jitter * (Math.random() * 2 - 1))));
}

function withTimeout<T>(p: Promise<T>, ms: number, name: string): Promise<T> {
  let t: NodeJS.Timeout;
  const timeout = new Promise<never>((_, rej) => { t = setTimeout(() => rej(Object.assign(new Error(`activity "${name}" timed out after ${ms}ms`), { name: 'TimeoutError' })), ms); });
  return Promise.race([p, timeout]).finally(() => clearTimeout(t));
}

/** Unconsumed signals per name, in arrival order (consumption = SignalMatched events already in history). */
function signalQueues(events: HistoryEvent[]) {
  const received = new Map<string, HistoryEvent[]>();
  const matched = new Map<string, number>();
  for (const e of events) {
    if (e.type === 'SignalReceived') { const l = received.get(e.payload.name) ?? []; l.push(e); received.set(e.payload.name, l); }
    else if (e.type === 'SignalMatched') matched.set(e.payload.name, (matched.get(e.payload.name) ?? 0) + 1);
  }
  const out = new Map<string, HistoryEvent[]>();
  for (const [name, list] of received) out.set(name, list.slice(matched.get(name) ?? 0));
  return out;
}

export type { WorkflowDefinition };
