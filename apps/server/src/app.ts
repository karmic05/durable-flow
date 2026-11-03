import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Worker as Thread } from 'node:worker_threads';
import cors from '@fastify/cors';
import fastifyStatic from '@fastify/static';
import Fastify from 'fastify';
import { replay, Store } from '@durable-flow/engine';
import { buildRegistry, SAMPLES } from '@durable-flow/examples';
import { Chaos } from './chaos.js';
import { checkInvariants } from './invariants.js';
import { Supervisor } from './supervisor.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(here, '../../..');

export interface AppOptions { databaseUrl: string; port: number; workers?: number; leaseMs?: number; publicDemo?: boolean }

export async function createApp(opts: AppOptions) {
  process.setSourceMapsEnabled(true);
  const store = new Store(opts.databaseUrl, 12);
  await store.migrate();
  const registry = buildRegistry();
  const sup = new Supervisor(store, opts.databaseUrl, opts.workers ?? 3, opts.leaseMs ?? 4000);

  const shortId = () => Math.random().toString(36).slice(2, 8);
  const startWorkflow = async (type: string, input: any) => {
    const id = `${type}-${shortId()}`;
    await store.createWorkflow({ id, type, input, codeHash: registry.workflows.get(type)?.hash });
    return id;
  };
  const startRandomSample = async () => {
    const pool = SAMPLES.filter((s) => s.id !== 'order-approval' && s.id !== 'loop');
    const s = pool[Math.floor(Math.random() * pool.length)];
    await startWorkflow(s.type, s.input());
  };
  const chaos = new Chaos(store, sup, startRandomSample);

  const app = Fastify({ logger: false });
  await app.register(cors, { origin: true });

  // ---- tiny in-memory rate limiter for mutating routes (public demo safety) ----
  const hits = new Map<string, number[]>();
  app.addHook('onRequest', async (req, reply) => {
    if (req.method === 'GET' || !req.url.startsWith('/api/')) return;
    const now = Date.now(); const k = req.ip;
    const list = (hits.get(k) ?? []).filter((t) => now - t < 60_000);
    list.push(now); hits.set(k, list);
    if (list.length > (opts.publicDemo ? 90 : 1000)) return reply.code(429).send({ error: 'rate limited, slow down' });
  });

  // ---------------------------------------------------------------- workflows
  app.get('/api/samples', async () => SAMPLES.map(({ id, label, type, blurb }) => ({ id, label, type, blurb })));
  app.get('/api/workflow-types', async () => [...registry.workflows.keys()]);

  app.post<{ Params: { id: string } }>('/api/samples/:id/start', async (req, reply) => {
    const s = SAMPLES.find((x) => x.id === req.params.id);
    if (!s) return reply.code(404).send({ error: 'unknown sample' });
    if ((await store.listWorkflows({ status: 'running', limit: 200 })).length > 60) return reply.code(429).send({ error: 'too many running workflows' });
    return { id: await startWorkflow(s.type, s.input()) };
  });

  app.post<{ Body: { type: string; input?: any } }>('/api/workflows', async (req, reply) => {
    if (!registry.workflows.has(req.body.type)) return reply.code(400).send({ error: 'unknown workflow type' });
    return { id: await startWorkflow(req.body.type, req.body.input ?? {}) };
  });

  app.get<{ Querystring: { status?: string; limit?: string } }>('/api/workflows', async (req) => {
    const rows = await store.listWorkflows({ status: req.query.status, limit: Number(req.query.limit ?? 100), rootOnly: true });
    return rows.map((w) => ({ ...w, codeChanged: !!w.code_hash && !!registry.workflows.get(w.type) && registry.workflows.get(w.type)!.hash !== w.code_hash }));
  });

  app.get<{ Params: { id: string } }>('/api/workflows/:id', async (req, reply) => {
    const wf = await store.getWorkflow(decodeURIComponent(req.params.id));
    if (!wf) return reply.code(404).send({ error: 'not found' });
    const def = registry.workflows.get(wf.type);
    return {
      workflow: wf, children: await store.listChildren(wf.id),
      codeChanged: !!wf.code_hash && !!def && def.hash !== wf.code_hash, currentHash: def?.hash ?? null,
    };
  });

  app.get<{ Params: { id: string } }>('/api/workflows/:id/events', async (req) => store.getEvents(decodeURIComponent(req.params.id)));

  app.get<{ Params: { id: string }; Querystring: { upToSeq?: string } }>('/api/workflows/:id/state', async (req, reply) => {
    const id = decodeURIComponent(req.params.id);
    const wf = await store.getWorkflow(id);
    if (!wf) return reply.code(404).send({ error: 'not found' });
    const def = registry.workflows.get(wf.type);
    if (!def) return reply.code(400).send({ error: 'unknown workflow type' });
    const upTo = req.query.upToSeq ? Number(req.query.upToSeq) : undefined;
    const events = await store.getEvents(id, upTo);
    const t0 = performance.now();
    const r = await replay(def.fn, events, { workflowId: id, captureSites: true });
    const replayMs = performance.now() - t0;

    const queries: Record<string, any> = {};
    for (const [name, fn] of r.queryHandlers) { try { queries[name] = fn(); } catch (e: any) { queries[name] = { error: String(e?.message ?? e) }; } }

    const last = events[events.length - 1];
    const cur = typeof last?.payload?.cmd === 'number' ? r.commands.find((c) => c.cmd === last.payload.cmd) : undefined;
    const parked = r.pending[0] ? r.commands.find((c) => c.cmd === r.pending[0].cmd) : undefined;
    const cursor = cur?.site ?? parked?.site;

    // Source of the workflow function with line numbers (only files inside this repo).
    let source: { file: string; lines: string[]; start: number; end: number } | undefined;
    const siteFile = r.commands.find((c) => c.site)?.site?.file;
    if (siteFile) {
      const abs = path.resolve(siteFile);
      if (abs.startsWith(REPO) && fs.existsSync(abs)) {
        const lines = fs.readFileSync(abs, 'utf8').split(/\r?\n/);
        const fnName = def.fn.name;
        let start = lines.findIndex((l) => new RegExp(`function\\s+${fnName}\\b`).test(l));
        if (start < 0) start = 0;
        let end = start; while (end < lines.length - 1 && !/^}/.test(lines[end])) end++;
        source = { file: path.relative(REPO, abs).replace(/\\/g, '/'), lines, start: start + 1, end: end + 1 };
      }
    }
    return {
      seq: last?.seq ?? 0, totalEvents: (await store.getEvents(id)).length, replayMs, eventsReplayed: r.eventsReplayed,
      done: r.done, output: r.output, error: r.error, nonDeterminism: r.nonDeterminism, continueAsNew: r.continueAsNew,
      commands: r.commands, pending: r.pending, saga: r.sagaStack, patches: r.patches, nextCmd: r.nextCmd, queries,
      wouldEmit: r.newEvents.map((e) => ({ type: e.type, cmd: e.payload.cmd })), cursor, source, currentEvent: last ?? null,
    };
  });

  app.get<{ Params: { id: string; name: string } }>('/api/workflows/:id/query/:name', async (req, reply) => {
    const id = decodeURIComponent(req.params.id);
    const wf = await store.getWorkflow(id);
    const def = wf && registry.workflows.get(wf.type);
    if (!wf || !def) return reply.code(404).send({ error: 'not found' });
    const r = await replay(def.fn, await store.getEvents(id), { workflowId: id });
    const h = r.queryHandlers.get(req.params.name);
    if (!h) return reply.code(404).send({ error: `no query handler "${req.params.name}"` });
    return { result: h() };
  });

  app.post<{ Params: { id: string }; Body: { name: string; payload?: any } }>('/api/workflows/:id/signal', async (req, reply) => {
    const ok = await store.signal(decodeURIComponent(req.params.id), req.body.name, req.body.payload ?? null);
    return ok ? { ok } : reply.code(409).send({ error: 'workflow is not running' });
  });

  app.post<{ Params: { id: string } }>('/api/workflows/:id/retry', async (req) => { await store.unstick(decodeURIComponent(req.params.id)); return { ok: true }; });

  app.post<{ Params: { id: string } }>('/api/workflows/:id/kill-holder', async (req, reply) => {
    const wf = await store.getWorkflow(decodeURIComponent(req.params.id));
    if (!wf?.lease_owner) return reply.code(409).send({ error: 'no worker currently holds this workflow' });
    return sup.kill(wf.lease_owner) ? { killed: wf.lease_owner } : reply.code(409).send({ error: 'worker not found' });
  });

  app.post<{ Params: { id: string } }>('/api/workflows/:id/fuzz', async (req, reply) => {
    const id = decodeURIComponent(req.params.id);
    const wf = await store.getWorkflow(id);
    if (!wf) return reply.code(404).send({ error: 'not found' });
    const events = await store.getEvents(id);
    return new Promise((resolve) => {
      const t = new Thread(new URL('./fuzz-thread.ts', import.meta.url), { workerData: { type: wf.type, events, runs: 25 }, execArgv: ['--import', 'tsx'] });
      const timer = setTimeout(() => { t.terminate(); resolve({ error: 'fuzzer timed out' }); }, 20_000);
      t.on('message', (m) => { clearTimeout(timer); resolve(m); t.terminate(); });
      t.on('error', (e) => { clearTimeout(timer); resolve({ error: String(e.message) }); });
    });
  });

  app.get<{ Params: { id: string } }>('/api/workflows/:id/trace', async (req, reply) => {
    const wf = await store.getWorkflow(decodeURIComponent(req.params.id));
    if (!wf) return reply.code(404).send({ error: 'not found' });
    const wfs = await store.pool.query(`SELECT id,type,status,created_at,updated_at,parent_id FROM workflows WHERE root_id=$1 ORDER BY created_at`, [wf.root_id]);
    const spans = await store.pool.query(
      `SELECT workflow_id,trace_id,span_id,parent_span_id,name,start_ms,end_ms,status,attributes FROM spans
       WHERE workflow_id = ANY($1) ORDER BY start_ms`, [wfs.rows.map((w: any) => w.id)]);
    const takeovers = await store.pool.query(
      `SELECT workflow_id, ts, payload FROM events WHERE type='WorkerTakeover' AND workflow_id = ANY($1) ORDER BY ts`, [wfs.rows.map((w: any) => w.id)]);
    return { rootId: wf.root_id, workflows: wfs.rows, spans: spans.rows, takeovers: takeovers.rows };
  });

  // ---------------------------------------------------------------- workers / chaos / settings
  app.get('/api/workers', async () => store.listWorkers());
  app.post('/api/workers/spawn', async () => ({ id: sup.spawnWorker() }));
  app.post<{ Params: { id: string } }>('/api/workers/:id/kill', async (req, reply) =>
    sup.kill(req.params.id) ? { killed: req.params.id } : reply.code(404).send({ error: 'no such live worker' }));

  app.post<{ Body: { activity: string } }>('/api/crash/arm', async (req) => { await store.setSetting('armed_crash', req.body.activity); return { armed: req.body.activity }; });
  app.delete('/api/crash/arm', async () => { await store.setSetting('armed_crash', null); return { armed: null }; });

  app.get('/api/settings', async () => {
    const s = await store.allSettings();
    return { idempotency: s.idempotency !== false, failure_rate: s.failure_rate ?? 0, speed: s.speed ?? 1, armed_crash: s.armed_crash ?? null };
  });
  app.post<{ Body: Record<string, any> }>('/api/settings', async (req) => {
    const b = req.body;
    if (typeof b.idempotency === 'boolean') await store.setSetting('idempotency', b.idempotency);
    if (typeof b.failure_rate === 'number') await store.setSetting('failure_rate', Math.min(0.6, Math.max(0, b.failure_rate)));
    if (typeof b.speed === 'number') await store.setSetting('speed', Math.min(4, Math.max(0.25, b.speed)));
    return { ok: true };
  });

  app.get('/api/chaos', async () => chaos.status());
  app.post<{ Body: { enabled: boolean } }>('/api/chaos', async (req) => { if (req.body.enabled) await chaos.start(); else await chaos.stop(); return chaos.status(); });
  app.get('/api/invariants', async () => checkInvariants(store));

  app.get<{ Querystring: { limit?: string } }>('/api/ledger', async (req) => {
    const limit = Number(req.query.limit ?? 80);
    const effects = await store.pool.query(
      `SELECT e.id, e.idempotency_key AS key, e.service, e.action, e.workflow_id, e.protected, e.ts,
              (SELECT count(*)::int FROM effect_calls c WHERE c.idempotency_key = e.idempotency_key) AS calls,
              (SELECT count(*)::int FROM side_effects d WHERE d.idempotency_key = e.idempotency_key) AS copies
       FROM side_effects e ORDER BY e.id DESC LIMIT $1`, [limit]);
    const totals = await store.pool.query(
      `SELECT (SELECT count(*)::int FROM side_effects) AS effects,
              (SELECT count(*)::int FROM effect_calls) AS calls,
              (SELECT count(*)::int FROM effect_calls WHERE deduped) AS deduped,
              (SELECT count(*)::int FROM (SELECT 1 FROM side_effects GROUP BY idempotency_key HAVING count(*)>1) x) AS duplicated_keys`);
    return { effects: effects.rows, totals: totals.rows[0] };
  });

  app.post('/api/reset', async () => { await store.resetAll(); return { ok: true }; });

  app.get('/api/bench', async (_req, reply) => {
    const f = path.join(REPO, 'bench-results.json');
    return fs.existsSync(f) ? JSON.parse(fs.readFileSync(f, 'utf8')) : reply.code(404).send({ error: 'run `npm run bench` first' });
  });

  // ---------------------------------------------------------------- live stream (SSE)
  const clients = new Set<import('node:http').ServerResponse>();
  const send = (res: import('node:http').ServerResponse, event: string, data: unknown) => res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  const broadcast = (event: string, data: unknown) => clients.forEach((c) => send(c, event, data));
  sup.onLog = (l) => broadcast('log', l);

  app.get('/api/stream', (req, reply) => {
    reply.hijack();
    const res = reply.raw;
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform', Connection: 'keep-alive',
      'Access-Control-Allow-Origin': '*', 'X-Accel-Buffering': 'no' });
    res.write(': hello\n\n');
    send(res, 'logs', sup.logs.slice(-40));
    clients.add(res);
    req.raw.on('close', () => clients.delete(res));
  });

  let lastEventId = await store.maxEventId();
  setInterval(async () => {
    if (!clients.size) { lastEventId = await store.maxEventId().catch(() => lastEventId); return; }
    try {
      const evs = await store.eventsAfter(lastEventId, 300);
      if (evs.length) { lastEventId = evs[evs.length - 1].id; broadcast('events', evs); }
    } catch { /* transient */ }
  }, 200).unref();

  setInterval(async () => {
    if (!clients.size) return;
    try {
      const [workers, settings, ledger] = await Promise.all([
        store.listWorkers(), store.allSettings(),
        store.pool.query(`SELECT (SELECT count(*)::int FROM side_effects) AS effects, (SELECT count(*)::int FROM effect_calls WHERE deduped) AS deduped,
                                 (SELECT count(*)::int FROM (SELECT 1 FROM side_effects GROUP BY idempotency_key HAVING count(*)>1) x) AS duplicated_keys`),
      ]);
      const counts = await store.pool.query(`SELECT status, count(*)::int c FROM workflows WHERE parent_id IS NULL GROUP BY 1`);
      broadcast('snapshot', {
        workers, leaseMs: opts.leaseMs ?? 4000, ledger: ledger.rows[0], chaos: chaos.status(),
        settings: { idempotency: settings.idempotency !== false, failure_rate: settings.failure_rate ?? 0, speed: settings.speed ?? 1, armed_crash: settings.armed_crash ?? null },
        counts: Object.fromEntries(counts.rows.map((r: any) => [r.status, r.c])), serverNow: Date.now(),
      });
    } catch { /* transient */ }
  }, 700).unref();

  // ---------------------------------------------------------------- static web build
  const dist = path.join(REPO, 'apps/web/dist');
  if (fs.existsSync(dist)) {
    await app.register(fastifyStatic, { root: dist, wildcard: false });
    app.setNotFoundHandler((req, reply) => (req.url.startsWith('/api/') ? reply.code(404).send({ error: 'not found' }) : reply.sendFile('index.html')));
  }

  return {
    app, store, sup, chaos,
    async start() {
      sup.ensureDesired();
      await app.listen({ port: opts.port, host: '0.0.0.0' });
    },
    async close() { await chaos.stop().catch(() => {}); await sup.shutdown(); await app.close(); await store.close(); },
  };
}
