import { ChildProcess, spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Store } from '@durable-flow/engine';

const here = path.dirname(fileURLToPath(import.meta.url));
const WORKER_ENTRY = path.join(here, 'worker-main.ts');
const LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';

export interface LogLine { ts: number; worker: string; text: string }

/** Spawns and kills worker processes; keeps the pool at a desired size by respawning crashed workers. */
export class Supervisor {
  private children = new Map<string, ChildProcess>();
  private counter = 0;
  private shuttingDown = false;
  desired: number;
  autoRespawn = true;
  respawnDelayMs = 2500;
  kills = 0;
  logs: LogLine[] = [];
  onLog?: (l: LogLine) => void;

  constructor(private store: Store, private databaseUrl: string, desired = 3, private leaseMs = 4000) { this.desired = desired; }

  private nextId() {
    const n = this.counter++;
    return n < 26 ? LETTERS[n] : `${LETTERS[n % 26]}${Math.floor(n / 26)}`;
  }

  private pushLog(worker: string, text: string) {
    const l = { ts: Date.now(), worker, text };
    this.logs.push(l); if (this.logs.length > 300) this.logs.shift();
    this.onLog?.(l);
  }

  spawnWorker(): string {
    const id = this.nextId();
    const child = spawn(process.execPath, ['--import', 'tsx', WORKER_ENTRY], {
      env: { ...process.env, WORKER_ID: id, DATABASE_URL: this.databaseUrl, LEASE_MS: String(this.leaseMs) },
      stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true,
    });
    this.children.set(id, child);
    const onData = (buf: Buffer) => buf.toString().split(/\r?\n/).filter(Boolean).forEach((line) => {
      const t = line.replace(/^\[[^\]]+\]\s*/, '');
      if (/TAKEOVER|armed crash|lost lease|NON-DETERMINISM|online|error/i.test(line)) this.pushLog(id, t);
    });
    child.stdout?.on('data', onData); child.stderr?.on('data', onData);
    child.on('exit', (code, signal) => {
      this.children.delete(id);
      this.store.markWorkerDead(id).catch(() => {});
      if (this.shuttingDown) return;
      this.pushLog(id, signal ? `process killed (${signal})` : `process exited (code ${code})`);
      if (this.autoRespawn) setTimeout(() => this.ensureDesired(), this.respawnDelayMs);
    });
    return id;
  }

  ensureDesired() {
    if (this.shuttingDown) return;
    while (this.children.size < this.desired) this.spawnWorker();
  }

  /** SIGKILL: no handlers run, the lease is simply abandoned. */
  kill(id: string): boolean {
    const c = this.children.get(id);
    if (!c) return false;
    this.kills++;
    this.pushLog(id, 'SIGKILL sent');
    c.kill('SIGKILL');
    return true;
  }
  aliveIds() { return [...this.children.keys()]; }

  async shutdown() {
    this.shuttingDown = true;
    for (const c of this.children.values()) c.kill('SIGKILL');
    this.children.clear();
  }
}
