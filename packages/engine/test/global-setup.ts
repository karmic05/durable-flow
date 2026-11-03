import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { startEmbeddedPostgres } from '../src/embedded.js';

let stop: (() => Promise<void>) | undefined;
export async function setup({ provide }: any) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'df-test-pg-'));
  const pg = await startEmbeddedPostgres({ dir, port: 54331, persistent: false });
  stop = pg.stop;
  provide('pgBase', 'postgres://postgres:postgres@127.0.0.1:54331');
}
export async function teardown() { await stop?.(); }
declare module 'vitest' { export interface ProvidedContext { pgBase: string } }
