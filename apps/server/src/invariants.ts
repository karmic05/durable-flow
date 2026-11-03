import { Store } from '@durable-flow/engine';

export interface InvariantCheck { name: string; ok: boolean; detail: string }
export interface InvariantReport { ok: boolean; at: number; checks: InvariantCheck[] }

const TERMINAL = `('WorkflowCompleted','WorkflowFailed','WorkflowContinuedAsNew')`;

/** Safety + liveness properties that must hold no matter how many workers we kill. */
export async function checkInvariants(store: Store): Promise<InvariantReport> {
  const q = (sql: string) => store.pool.query(sql).then((r) => r.rows);
  const checks: InvariantCheck[] = [];

  const dup = await q(`SELECT idempotency_key k, count(*)::int c FROM side_effects GROUP BY 1 HAVING count(*) > 1 LIMIT 5`);
  checks.push({ name: 'No duplicate side effects', ok: dup.length === 0,
    detail: dup.length ? `duplicated: ${dup.map((d: any) => `${d.k} x${d.c}`).join(', ')}` : 'every idempotency key has exactly one effect' });

  const gaps = await q(`SELECT workflow_id FROM events GROUP BY workflow_id HAVING max(seq) <> count(*) LIMIT 5`);
  checks.push({ name: 'Histories are gap-free', ok: gaps.length === 0,
    detail: gaps.length ? `gaps in ${gaps.map((g: any) => g.workflow_id).join(', ')}` : 'seq 1..N contiguous in every history' });

  const term = await q(`SELECT w.id FROM workflows w LEFT JOIN events e ON e.workflow_id=w.id AND e.type IN ${TERMINAL}
                        WHERE w.status IN ('completed','failed') GROUP BY w.id HAVING count(e.seq) <> 1 LIMIT 5`);
  checks.push({ name: 'Exactly one terminal event per finished workflow', ok: term.length === 0,
    detail: term.length ? `bad: ${term.map((t: any) => t.id).join(', ')}` : 'no double-completions' });

  const orphan = await q(`SELECT a.workflow_id, a.payload->>'cmd' cmd FROM events a
                          LEFT JOIN events s ON s.workflow_id=a.workflow_id AND s.type='ActivityScheduled' AND s.payload->>'cmd' = a.payload->>'cmd'
                          WHERE a.type='ActivityCompleted' AND s.seq IS NULL LIMIT 5`);
  checks.push({ name: 'Every result has a matching command', ok: orphan.length === 0,
    detail: orphan.length ? 'completion without schedule' : 'no orphaned results' });

  const aliveWorkers = await q(`SELECT count(*)::int c FROM workers WHERE status='alive' AND last_seen > now() - interval '3 seconds'`);
  const stalled = await q(`SELECT id FROM workflows WHERE status='running' AND lease_expires_at < now() - interval '15 seconds' LIMIT 5`);
  const live = aliveWorkers[0].c === 0 || stalled.length === 0;
  checks.push({ name: 'No workflow stranded on a dead lease', ok: live,
    detail: live ? 'dead leases are reclaimed' : `stranded: ${stalled.map((s: any) => s.id).join(', ')}` });

  return { ok: checks.every((c) => c.ok), at: Date.now(), checks };
}
