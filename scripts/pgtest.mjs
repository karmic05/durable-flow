import EmbeddedPostgres from 'embedded-postgres';
import pg from 'pg';
const dir = process.env.TEMP + '/df-pgtest-' + Date.now();
const e = new EmbeddedPostgres({ databaseDir: dir, user: 'postgres', password: 'postgres', port: 54329, persistent: false, onLog: () => {}, onError: () => {} });
await e.initialise(); await e.start();
const c = new pg.Client({ connectionString: 'postgres://postgres:postgres@127.0.0.1:54329/postgres' });
await c.connect(); console.log((await c.query('select version()')).rows[0].version); await c.end();
await e.stop(); console.log('ok');
