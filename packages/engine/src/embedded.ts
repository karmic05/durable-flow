import fs from 'node:fs';
import path from 'node:path';
import EmbeddedPostgres from 'embedded-postgres';

export interface EmbeddedPg {
  url: (db?: string) => string;
  createDatabase: (name: string) => Promise<string>;
  stop: () => Promise<void>;
}

/** Real Postgres, downloaded as an npm binary. Used for local dev and tests (no Docker needed). */
export async function startEmbeddedPostgres(o: { dir: string; port?: number; persistent?: boolean }): Promise<EmbeddedPg> {
  const port = o.port ?? 54329;
  const e = new EmbeddedPostgres({
    databaseDir: o.dir, user: 'postgres', password: 'postgres', port, persistent: o.persistent ?? true,
    onLog: () => {}, onError: () => {},
  });
  if (!fs.existsSync(path.join(o.dir, 'PG_VERSION'))) await e.initialise();
  await e.start();
  const url = (db = 'postgres') => `postgres://postgres:postgres@127.0.0.1:${port}/${db}`;
  return {
    url,
    createDatabase: async (name) => { try { await e.createDatabase(name); } catch { /* already exists */ } return url(name); },
    stop: () => e.stop(),
  };
}
