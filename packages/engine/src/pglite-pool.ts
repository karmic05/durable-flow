import type { ClientLike, PoolLike, QueryResultLike } from './store-base.js';

/**
 * Structural type of a PGlite instance (Postgres compiled to WebAssembly), so the engine
 * needs no dependency on it. Pass `new PGlite()` straight in.
 */
export interface PGliteLike {
  query(sql: string, params?: any[]): Promise<{ rows: any[]; affectedRows?: number }>;
  exec(sql: string): Promise<Array<{ rows: any[]; affectedRows?: number }>>;
  close?(): Promise<void>;
}

/** FIFO async mutex. PGlite has exactly one connection, so transactions must not interleave. */
class Mutex {
  private tail: Promise<void> = Promise.resolve();
  lock(): Promise<() => void> {
    let release!: () => void;
    const next = new Promise<void>((r) => { release = r; });
    const prev = this.tail;
    this.tail = prev.then(() => next);
    return prev.then(() => release);
  }
}

/**
 * Presents a single-connection PGlite database as a `pg`-style pool, so the real engine
 * (leases, fenced appends, SKIP LOCKED claims...) runs unchanged inside a browser tab.
 * `connect()` holds the lock until `release()`, which makes BEGIN..COMMIT blocks atomic.
 */
export class PglitePool implements PoolLike {
  private mu = new Mutex();
  constructor(private db: PGliteLike) {}

  private async run(sql: string, params?: any[]): Promise<QueryResultLike> {
    const args = params?.map((p) => (p instanceof Date ? p.toISOString() : p));
    if (!args || args.length === 0) {
      // No parameters: simple protocol, which also allows multi-statement scripts (the schema).
      const res = await this.db.exec(sql);
      const last = res[res.length - 1] ?? { rows: [], affectedRows: 0 };
      return { rows: last.rows, rowCount: Math.max(last.affectedRows ?? 0, last.rows.length) };
    }
    const r = await this.db.query(sql, args);
    return { rows: r.rows, rowCount: Math.max(r.affectedRows ?? 0, r.rows.length) };
  }

  async query(sql: string, params?: any[]): Promise<QueryResultLike> {
    const done = await this.mu.lock();
    try { return await this.run(sql, params); } finally { done(); }
  }

  async connect(): Promise<ClientLike> {
    const done = await this.mu.lock();
    let released = false;
    return {
      query: (sql, params) => this.run(sql, params),
      release: () => { if (!released) { released = true; done(); } },
    };
  }

  async end() { await this.db.close?.(); }
}
