import pg from 'pg';
import { SCHEMA } from './schema.js';
import { EventDraft, HistoryEvent, LeaseLostError, WorkflowRow } from './types.js';

const { Pool } = pg;
export type Queryable = pg.Pool | pg.PoolClient;

function rowToEvent(r: any): HistoryEvent {
  return { seq: r.seq, type: r.type, payload: r.payload, ts: new Date(r.ts).toISOString() };
}
function iso(v: any) { return v ? new Date(v).toISOString() : null; }
function rowToWf(r: any): WorkflowRow {
  return {
    ...r,
    lease_epoch: Number(r.lease_epoch),
    lease_expires_at: iso(r.lease_expires_at),
    next_wake_at: !(r.next_wake_at instanceof Date) || isNaN(r.next_wake_at.getTime()) ? 'infinity' : r.next_wake_at.toISOString(),
    created_at: iso(r.created_at)!,
    updated_at: iso(r.updated_at)!,
  };
}

export interface ClaimResult { wf: WorkflowRow; prevOwner: string | null }

export class Store {
  pool: pg.Pool;
  constructor(public url: string, max = 10) {
    this.pool = new Pool({ connectionString: url, max });
    this.pool.on('error', () => { /* idle client errors are non-fatal */ });
  }
  async migrate() { await this.pool.query(SCHEMA); }
  async close() { await this.pool.end(); }

  // ---------- workflows ----------
  async createWorkflow(o: {
    id: string; type: string; input: any; codeHash?: string; parentId?: string; parentCmd?: number; rootId?: string;
  }): Promise<{ created: boolean }> {
    const c = await this.pool.connect();
    try {
      await c.query('BEGIN');
      const r = await c.query(
        `INSERT INTO workflows (id,type,input,parent_id,parent_cmd,root_id,code_hash,next_seq)
         VALUES ($1,$2,$3,$4,$5,$6,$7,2) ON CONFLICT (id) DO NOTHING RETURNING id`,
        [o.id, o.type, JSON.stringify(o.input ?? null), o.parentId ?? null, o.parentCmd ?? null, o.rootId ?? o.id, o.codeHash ?? null]);
      if (r.rowCount === 0) { await c.query('ROLLBACK'); return { created: false }; }
      await c.query(`INSERT INTO events (workflow_id,seq,type,payload) VALUES ($1,1,'WorkflowStarted',$2)`,
        [o.id, JSON.stringify({ workflowType: o.type, input: o.input ?? null, codeHash: o.codeHash ?? null, parentId: o.parentId ?? null })]);
      await c.query('COMMIT');
      return { created: true };
    } catch (e) { await c.query('ROLLBACK').catch(() => {}); throw e; } finally { c.release(); }
  }

  async getWorkflow(id: string): Promise<WorkflowRow | null> {
    const r = await this.pool.query('SELECT * FROM workflows WHERE id=$1', [id]);
    return r.rows[0] ? rowToWf(r.rows[0]) : null;
  }
  async listWorkflows(opts: { status?: string; limit?: number; rootOnly?: boolean } = {}): Promise<WorkflowRow[]> {
    const where: string[] = []; const args: any[] = [];
    if (opts.status) { args.push(opts.status); where.push(`status=$${args.length}`); }
    if (opts.rootOnly) where.push('parent_id IS NULL');
    args.push(opts.limit ?? 100);
    const r = await this.pool.query(
      `SELECT * FROM workflows ${where.length ? 'WHERE ' + where.join(' AND ') : ''} ORDER BY created_at DESC LIMIT $${args.length}`, args);
    return r.rows.map(rowToWf);
  }
  async listChildren(parentId: string): Promise<WorkflowRow[]> {
    const r = await this.pool.query('SELECT * FROM workflows WHERE parent_id=$1 ORDER BY created_at', [parentId]);
    return r.rows.map(rowToWf);
  }

  // ---------- leasing ----------
  async claim(workerId: string, leaseMs: number): Promise<ClaimResult | null> {
    const r = await this.pool.query(
      `WITH c AS (
         SELECT id, lease_owner AS prev_owner FROM workflows
         WHERE status='running'
           AND ((lease_expires_at IS NULL AND next_wake_at <= now()) OR lease_expires_at < now())
         ORDER BY next_wake_at LIMIT 1 FOR UPDATE SKIP LOCKED)
       UPDATE workflows w SET lease_owner=$1, lease_expires_at=now() + $2 * interval '1 millisecond',
         lease_epoch=w.lease_epoch+1, updated_at=now()
       FROM c WHERE w.id=c.id RETURNING w.*, c.prev_owner`, [workerId, leaseMs]);
    if (!r.rows[0]) return null;
    const { prev_owner, ...row } = r.rows[0];
    return { wf: rowToWf(row), prevOwner: prev_owner };
  }
  /** Extend lease. Returns false if we no longer own it (fenced out). */
  async heartbeat(id: string, epoch: number, workerId: string, leaseMs: number): Promise<boolean> {
    const r = await this.pool.query(
      `UPDATE workflows SET lease_expires_at=now() + $4 * interval '1 millisecond'
       WHERE id=$1 AND lease_epoch=$2 AND lease_owner=$3 AND status='running'`, [id, epoch, workerId, leaseMs]);
    return r.rowCount === 1;
  }
  /**
   * Release lease and schedule next wake. If events were appended after `lastSeq`
   * (e.g. a signal arrived while we were deciding to park) we re-wake immediately.
   */
  async release(id: string, epoch: number, lastSeq: number, wakeAt: Date | null) {
    await this.pool.query(
      `UPDATE workflows SET lease_owner=NULL, lease_expires_at=NULL, updated_at=now(),
         next_wake_at = CASE WHEN next_seq = $3 + 1 THEN COALESCE($4::timestamptz, 'infinity'::timestamptz) ELSE now() END
       WHERE id=$1 AND lease_epoch=$2`, [id, epoch, lastSeq, wakeAt]);
  }

  // ---------- events ----------
  async getEvents(id: string, upToSeq?: number): Promise<HistoryEvent[]> {
    const r = upToSeq === undefined
      ? await this.pool.query('SELECT seq,type,payload,ts FROM events WHERE workflow_id=$1 ORDER BY seq', [id])
      : await this.pool.query('SELECT seq,type,payload,ts FROM events WHERE workflow_id=$1 AND seq<=$2 ORDER BY seq', [id, upToSeq]);
    return r.rows.map(rowToEvent);
  }
  async eventsAfter(globalId: number, limit = 500) {
    const r = await this.pool.query(
      `SELECT id, workflow_id, seq, type, payload, ts FROM events WHERE id > $1 ORDER BY id LIMIT $2`, [globalId, limit]);
    return r.rows.map((x: any) => ({ id: Number(x.id), workflowId: x.workflow_id as string, ...rowToEvent(x) }));
  }
  async maxEventId(): Promise<number> {
    const r = await this.pool.query('SELECT coalesce(max(id),0) AS m FROM events');
    return Number(r.rows[0].m);
  }

  private async insertEvents(c: Queryable, id: string, first: number, drafts: EventDraft[]) {
    if (!drafts.length) return;
    const vals: any[] = [id]; const rows: string[] = [];
    drafts.forEach((d, i) => {
      vals.push(first + i, d.type, JSON.stringify(d.payload));
      rows.push(`($1,$${vals.length - 2},$${vals.length - 1},$${vals.length}::jsonb)`);
    });
    await c.query(`INSERT INTO events (workflow_id,seq,type,payload) VALUES ${rows.join(',')}`, vals);
  }

  /**
   * Atomically append events. With `epoch`, the append is FENCED: it only succeeds while
   * we still hold the lease at that epoch. Without it (signals, child results) it appends
   * only if the workflow is still running and wakes it.
   */
  async append(id: string, epoch: number | null, drafts: EventDraft[]): Promise<HistoryEvent[]> {
    if (!drafts.length) return [];
    const c = await this.pool.connect();
    try {
      await c.query('BEGIN');
      const out = await this.appendTx(c, id, epoch, drafts);
      await c.query('COMMIT');
      return out;
    } catch (e) { await c.query('ROLLBACK').catch(() => {}); throw e; } finally { c.release(); }
  }
  private async appendTx(c: Queryable, id: string, epoch: number | null, drafts: EventDraft[]): Promise<HistoryEvent[]> {
    const r = await c.query(
      `UPDATE workflows SET next_seq = next_seq + $2, updated_at=now()
         ${epoch === null ? ', next_wake_at = LEAST(next_wake_at, now())' : ''}
       WHERE id=$1 AND status='running' ${epoch !== null ? 'AND lease_epoch=$3' : ''}
       RETURNING next_seq - $2 AS first`, epoch !== null ? [id, drafts.length, epoch] : [id, drafts.length]);
    if (!r.rows[0]) throw new LeaseLostError();
    const first = Number(r.rows[0].first);
    await this.insertEvents(c, id, first, drafts);
    const now = new Date().toISOString();
    return drafts.map((d, i) => ({ seq: first + i, type: d.type, payload: d.payload, ts: now }));
  }

  /** Append a signal from outside. Returns false if the workflow is not running. */
  async signal(id: string, name: string, payload: any): Promise<boolean> {
    try { await this.append(id, null, [{ type: 'SignalReceived', payload: { name, payload } }]); return true; }
    catch (e) { if (e instanceof LeaseLostError) return false; throw e; }
  }

  /** Move a stuck workflow back to running so a worker re-tries replay (after a code fix). */
  async unstick(id: string) {
    await this.pool.query(
      `UPDATE workflows SET status='running', error=NULL, next_wake_at=now(), lease_owner=NULL, lease_expires_at=NULL
       WHERE id=$1 AND status='stuck'`, [id]);
  }

  /**
   * Terminal transition. Appends final events, flips status, and (atomically) notifies the parent workflow.
   * `continueAs` additionally creates the successor workflow in the same transaction.
   */
  async finish(wf: WorkflowRow, epoch: number, o: {
    status: 'completed' | 'failed' | 'stuck';
    result?: any; error?: any; events: EventDraft[];
    continueAs?: { id: string; type: string; input: any; codeHash?: string };
  }): Promise<HistoryEvent[]> {
    const c = await this.pool.connect();
    try {
      await c.query('BEGIN');
      // For 'stuck' we must keep status=running during the fenced append, then flip.
      const evs = await this.appendTx(c, wf.id, epoch, o.events);
      await c.query(
        `UPDATE workflows SET status=$2, result=$3, error=$4, lease_owner=NULL, lease_expires_at=NULL, updated_at=now()
         WHERE id=$1 AND lease_epoch=$5`,
        [wf.id, o.status, JSON.stringify(o.result ?? null), o.error ? JSON.stringify(o.error) : null, epoch]);
      if (o.continueAs) {
        const n = o.continueAs;
        await c.query(`INSERT INTO workflows (id,type,input,root_id,code_hash,next_seq,parent_id,parent_cmd)
                       VALUES ($1,$2,$3,$4,$5,2,$6,$7) ON CONFLICT (id) DO NOTHING`,
          [n.id, n.type, JSON.stringify(n.input ?? null), wf.root_id, n.codeHash ?? null, wf.parent_id, wf.parent_cmd]);
        await c.query(`INSERT INTO events (workflow_id,seq,type,payload) VALUES ($1,1,'WorkflowStarted',$2) ON CONFLICT DO NOTHING`,
          [n.id, JSON.stringify({ workflowType: n.type, input: n.input ?? null, codeHash: n.codeHash ?? null,
            continuedFrom: wf.id, parentId: wf.parent_id })]);
      } else if (wf.parent_id && wf.parent_cmd !== null && o.status !== 'stuck') {
        const done = o.status === 'completed';
        try {
          await this.appendTx(c, wf.parent_id, null, [{
            type: done ? 'ChildCompleted' : 'ChildFailed',
            payload: done ? { cmd: wf.parent_cmd, childId: wf.id, result: o.result ?? null }
                          : { cmd: wf.parent_cmd, childId: wf.id, error: o.error },
          }]);
        } catch (e) { if (!(e instanceof LeaseLostError)) throw e; /* parent already finished */ }
      }
      await c.query('COMMIT');
      return evs;
    } catch (e) { await c.query('ROLLBACK').catch(() => {}); throw e; } finally { c.release(); }
  }

  // ---------- workers ----------
  async upsertWorker(id: string, pid: number, active: string[]) {
    await this.pool.query(
      `INSERT INTO workers (id,pid,active,status,last_seen,started_at) VALUES ($1,$2,$3,'alive',now(),now())
       ON CONFLICT (id) DO UPDATE SET pid=$2, active=$3, status='alive', last_seen=now()`, [id, pid, JSON.stringify(active)]);
  }
  async markWorkerDead(id: string) {
    await this.pool.query(`UPDATE workers SET status='dead', active='[]' WHERE id=$1`, [id]);
  }
  async listWorkers() {
    const r = await this.pool.query(`SELECT *, extract(epoch from (now()-last_seen))*1000 AS age_ms FROM workers ORDER BY started_at`);
    return r.rows.map((w: any) => ({
      id: w.id as string, pid: w.pid as number, status: w.status as string, active: w.active as string[],
      startedAt: new Date(w.started_at).toISOString(), ageMs: Math.round(Number(w.age_ms)),
    }));
  }

  // ---------- settings ----------
  async getSetting<T = any>(key: string, def: T): Promise<T> {
    const r = await this.pool.query('SELECT value FROM settings WHERE key=$1', [key]);
    return r.rows[0] ? r.rows[0].value : def;
  }
  async setSetting(key: string, value: any) {
    await this.pool.query(`INSERT INTO settings (key,value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=$2`, [key, JSON.stringify(value)]);
  }
  /** Atomically consume a setting only if it currently equals `expected`. */
  async consumeSetting(key: string, expected: string): Promise<boolean> {
    const r = await this.pool.query(`UPDATE settings SET value='null'::jsonb WHERE key=$1 AND value = to_jsonb($2::text)`, [key, expected]);
    return r.rowCount === 1;
  }
  async allSettings(): Promise<Record<string, any>> {
    const r = await this.pool.query('SELECT key,value FROM settings');
    return Object.fromEntries(r.rows.map((x: any) => [x.key, x.value]));
  }

  async resetAll() {
    await this.pool.query(`TRUNCATE events, workflows, side_effects, effect_calls, spans RESTART IDENTITY CASCADE`);
  }
}
