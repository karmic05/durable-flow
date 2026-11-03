import { replay, WorkflowFn } from './replay.js';
import { HistoryEvent } from './types.js';

export interface FuzzReport {
  runs: number;
  deterministic: boolean;
  violations: { api: string; site?: string; count: number }[];
  divergence?: string;
  commandCount: number;
}

function site(): string | undefined {
  for (const l of (new Error().stack ?? '').split('\n').slice(1)) {
    if (/[\/]engine[\/]src[\/]/.test(l) || l.includes('node:internal')) continue;
    const m = l.match(/([^\s()]+:\d+):\d+\)?\s*$/);
    if (m) return m[1].split(/[\/]/).slice(-2).join('/');
  }
}

/**
 * Determinism fuzzer. Replays the recorded history repeatedly while:
 *  - trapping every nondeterministic global (Date.now, Math.random, crypto.randomUUID, timers, fetch, performance.now)
 *    and recording where workflow code touched them, and
 *  - perturbing their values on every run after the first,
 * then checks that the sequence of commands the code issues never changes.
 * MUST run in an isolated realm (worker thread) because it temporarily swaps globals.
 */
export async function fuzzReplay(fn: WorkflowFn, events: HistoryEvent[], runs = 25): Promise<FuzzReport> {
  const hits = new Map<string, { api: string; site?: string; count: number }>();
  const g = globalThis as any;
  const orig = {
    now: Date.now, random: Math.random, uuid: g.crypto?.randomUUID?.bind(g.crypto), st: g.setTimeout, si: g.setInterval,
    fetch: g.fetch, perf: performance.now.bind(performance),
  };
  let perturb = false;
  const hit = (api: string) => {
    const s = site(); const k = `${api}@${s}`;
    const h = hits.get(k) ?? { api, site: s, count: 0 }; h.count++; hits.set(k, h);
  };
  const install = () => {
    Date.now = () => { hit('Date.now()'); return orig.now() + (perturb ? Math.floor(orig.random() * 86_400_000) : 0); };
    Math.random = () => { hit('Math.random()'); return orig.random(); };
    if (g.crypto) g.crypto.randomUUID = () => { hit('crypto.randomUUID()'); return orig.uuid(); };
    g.setTimeout = (...a: any[]) => { hit('setTimeout()'); return orig.st(...a); };
    g.setInterval = (...a: any[]) => { hit('setInterval()'); return orig.si(...a); };
    if (orig.fetch) g.fetch = (...a: any[]) => { hit('fetch()'); return orig.fetch(...a); };
    performance.now = () => { hit('performance.now()'); return orig.perf() + (perturb ? Math.random() * 1000 : 0); };
  };
  const restore = () => {
    Date.now = orig.now; Math.random = orig.random; if (g.crypto) g.crypto.randomUUID = orig.uuid;
    g.setTimeout = orig.st; g.setInterval = orig.si; if (orig.fetch) g.fetch = orig.fetch; performance.now = orig.perf;
  };

  let baseline: string | undefined; let divergence: string | undefined; let count = 0;
  try {
    for (let i = 0; i < runs; i++) {
      perturb = i > 0;
      install();
      let r;
      try { r = await replay(fn, events, { workflowId: events[0] ? 'fuzz' : 'fuzz' }); } finally { restore(); }
      const sig = r.commands.map((c) => `${c.kind}:${c.name}`).join(' | ') + (r.done ? ' => done' : '') + (r.nonDeterminism ? ' => NONDET' : '');
      count = r.commands.length;
      if (baseline === undefined) baseline = sig;
      else if (sig !== baseline && !divergence) divergence = `run ${i} issued a different command sequence than run 0:\n  run 0: ${baseline}\n  run ${i}: ${sig}`;
    }
  } finally { restore(); }
  const violations = [...hits.values()].map((v) => ({ ...v, count: Math.ceil(v.count / runs) }));
  return { runs, deterministic: !divergence && violations.length === 0, violations, divergence, commandCount: count };
}
