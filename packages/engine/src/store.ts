import pg from 'pg';
import { PoolLike, StoreBase } from './store-base.js';

/** Node + Postgres (via `pg`). For the browser, build a StoreBase over PGlite instead. */
export class Store extends StoreBase {
  readonly url: string;
  constructor(url: string, max = 10) {
    const pool = new pg.Pool({ connectionString: url, max });
    pool.on('error', () => { /* idle client errors are non-fatal */ });
    super(pool as unknown as PoolLike);
    this.url = url;
  }
}
