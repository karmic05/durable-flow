import { Store } from '@durable-flow/engine';
import { checkInvariants, InvariantReport } from './invariants.js';
import { Supervisor } from './supervisor.js';

const CRASH_POINTS = ['reserveInventory', 'chargeCard', 'packOrder', 'dispatchCarrier', 'sendConfirmationEmail'];

/** Randomly kills workers and injects failures while continuously checking invariants. */
export class Chaos {
  enabled = false;
  kills = 0;
  armed = 0;
  started = 0;
  checksPassed = 0;
  checksFailed = 0;
  lastReport?: InvariantReport;
  private timers: NodeJS.Timeout[] = [];

  constructor(private store: Store, private sup: Supervisor, private startWorkflow: () => Promise<void>) {}

  status() {
    return { enabled: this.enabled, kills: this.kills, armedCrashes: this.armed, workflowsStarted: this.started,
      checksPassed: this.checksPassed, checksFailed: this.checksFailed, lastReport: this.lastReport };
  }

  async start() {
    if (this.enabled) return;
    this.enabled = true;
    await this.store.setSetting('failure_rate', 0.12);
    const every = (ms: number, fn: () => Promise<void>) => this.timers.push(setInterval(() => { fn().catch(() => {}); }, ms));

    every(2500, async () => {                                  // keep some load on the system
      const running = (await this.store.listWorkflows({ status: 'running', limit: 50 })).length;
      if (running < 6) { await this.startWorkflow(); this.started++; }
    });
    every(5000, async () => {                                  // violence
      const workers = (await this.store.listWorkers()).filter((w) => w.status === 'alive');
      const busy = workers.filter((w) => w.active.length > 0);
      if (Math.random() < 0.5) {
        await this.store.setSetting('armed_crash', CRASH_POINTS[Math.floor(Math.random() * CRASH_POINTS.length)]);
        this.armed++;
        setTimeout(() => { this.store.consumeSetting('armed_crash', 'x').catch(() => {}); }, 0);
      } else {
        const pool = busy.length ? busy : workers;
        if (pool.length > 1 && this.sup.kill(pool[Math.floor(Math.random() * pool.length)].id)) this.kills++;
      }
    });
    every(1500, async () => {                                  // verify
      const r = await checkInvariants(this.store);
      this.lastReport = r;
      if (r.ok) this.checksPassed++; else this.checksFailed++;
    });
  }

  async stop() {
    this.enabled = false;
    this.timers.forEach(clearInterval); this.timers = [];
    await this.store.setSetting('failure_rate', 0);
    await this.store.setSetting('armed_crash', null);
  }
}
