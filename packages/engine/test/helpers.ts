import pg from 'pg';
import { inject } from 'vitest';
import { Registry, Store, Worker, WorkerOptions } from '../src/index.js';

let n = 0;
export async function newStore(): Promise<Store> {
  const base = inject('pgBase');
  const name = `t${Date.now()}_${n++}`;
  const admin = new pg.Client({ connectionString: `${base}/postgres` });
  await admin.connect(); await admin.query(`CREATE DATABASE ${name}`); await admin.end();
  const store = new Store(`${base}/${name}`, 20);
  await store.migrate();
  return store;
}
export const fast: Partial<WorkerOptions> = { leaseMs: 800, heartbeatMs: 200, pollMs: 25 };
export function worker(id: string, store: Store, registry: Registry, extra: Partial<WorkerOptions> = {}) {
  return new Worker({ id, store, registry, ...fast, ...extra });
}
export async function waitFor<T>(fn: () => Promise<T | undefined | false>, ms = 15000, every = 40): Promise<T> {
  const end = Date.now() + ms;
  for (;;) {
    const v = await fn();
    if (v) return v as T;
    if (Date.now() > end) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, every));
  }
}
export const status = (store: Store, id: string) => async () => {
  const w = await store.getWorkflow(id);
  return w && w.status !== 'running' ? w : undefined;
};
/** Idempotent "external service": one row per key, mirrors examples' ledger. */
export async function applyEffect(store: Store, key: string, service: string, protectedMode = true) {
  const r = await store.pool.query(
    `INSERT INTO side_effects (idempotency_key,service,action,protected) VALUES ($1,$2,'do',$3) ON CONFLICT DO NOTHING RETURNING id`,
    [key, service, protectedMode]);
  return r.rowCount === 1;
}
