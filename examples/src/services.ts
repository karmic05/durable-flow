import type { ActivityContext, Store } from '@durable-flow/engine';

/**
 * The simulated "outside world" (payments, inventory, carrier, email).
 *
 * Every call carries the engine-provided idempotency key (workflowId:commandSeq). It is stable across retries and
 * across worker takeovers, so when a crashed activity is re-executed the service recognises it and does NOT repeat
 * the effect. That is what turns the engine's at-least-once activity execution into exactly-once *effects*.
 *
 * The `idempotency` setting can be switched OFF in the UI to show the failure mode: the same crash then
 * produces a duplicate charge.
 */
export interface Settings { idempotency: boolean; failure_rate: number; speed: number; armed_crash: string | null }

let cache: { at: number; value: Settings } | undefined;
export async function getSettings(store: Store): Promise<Settings> {
  if (cache && Date.now() - cache.at < 250) return cache.value;
  const s = await store.allSettings();
  const value: Settings = {
    idempotency: s.idempotency !== false,
    failure_rate: typeof s.failure_rate === 'number' ? s.failure_rate : 0,
    speed: typeof s.speed === 'number' && s.speed > 0 ? s.speed : 1,
    armed_crash: typeof s.armed_crash === 'string' ? s.armed_crash : null,
  };
  cache = { at: Date.now(), value };
  return value;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function callService(
  ac: ActivityContext, service: string, action: string, payload: Record<string, unknown>, latencyMs = 900,
): Promise<{ deduped: boolean }> {
  const settings = await getSettings(ac.store);
  await sleep(latencyMs / settings.speed);                     // network latency of the external system
  const prot = settings.idempotency;
  const r = await ac.store.pool.query(
    `INSERT INTO side_effects (idempotency_key, service, action, payload, protected, workflow_id)
     VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING RETURNING id`,
    [ac.idempotencyKey, service, action, JSON.stringify(payload), prot, ac.workflowId]);
  const deduped = r.rowCount === 0;
  await ac.store.pool.query(
    `INSERT INTO effect_calls (idempotency_key, service, action, deduped, workflow_id) VALUES ($1,$2,$3,$4,$5)`,
    [ac.idempotencyKey, service, action, deduped, ac.workflowId]);
  return { deduped };
}
