/**
 * Website playground: the REAL durable-flow engine running against REAL Postgres (PGlite, Postgres compiled to
 * WebAssembly) inside the visitor's browser. No server involved, so it keeps working with nobody's computer on.
 */
import { NonRetryableError, PglitePool, Registry, StoreBase, Worker } from '../../../packages/engine/src/browser';
import type { ActivityHandler, HistoryEvent } from '../../../packages/engine/src/browser';

const PGLITE_URL: string = 'https://cdn.jsdelivr.net/npm/@electric-sql/pglite@0.5.15/dist/index.js';
const $ = <T extends HTMLElement = HTMLElement>(s: string) => document.querySelector(s) as T;
const esc = (s: unknown) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]!));
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/* ───────────────────────── presets ───────────────────────── */
const PRESETS: Record<string, { label: string; crash: string; code: string; hint: string }> = {
  checkout: {
    label: 'Checkout saga (crash it)', crash: 'chargeCard',
    hint: 'Press Start: a worker dies right after charging the card, before that is recorded. Watch another worker take over.',
    code: `async function workflow(ctx, input) {
  const saga = ctx.saga()
  try {
    await ctx.activity('reserveInventory', { sku: 'keyboard' })
    saga.add('releaseInventory', { sku: 'keyboard' })

    await ctx.activity('chargeCard', { amount: 49 })
    saga.add('refundCard', { amount: 49 })

    await ctx.sleep(2000)            // a durable timer

    await ctx.activity('shipOrder', {})
    await ctx.activity('sendReceipt', {})
    return 'order complete'
  } catch (err) {
    await saga.compensate()          // undo, in reverse
    throw err
  }
}`,
  },
  retries: {
    label: 'Retries with backoff', crash: '',
    hint: 'Activities named flaky… fail their first two attempts. Every attempt, and the wait between them, is an event.',
    code: `async function workflow(ctx) {
  const answer = await ctx.activity('flakyWeatherApi', { city: 'Boston' }, {
    retry: { maxAttempts: 5, initialIntervalMs: 600, backoff: 2, jitter: 0 },
  })
  return answer
}`,
  },
  saga: {
    label: 'Saga rolls back', crash: '',
    hint: 'Activities named fail… fail permanently. Watch the undo steps run in reverse order.',
    code: `async function workflow(ctx) {
  const saga = ctx.saga()
  try {
    await ctx.activity('reserveInventory', {})
    saga.add('releaseInventory', {})
    await ctx.activity('chargeCard', {})
    saga.add('refundCard', {})
    await ctx.activity('failShipping', {})     // fails permanently
  } catch (err) {
    await saga.compensate()
    throw err
  }
}`,
  },
  approval: {
    label: 'Wait for human approval', crash: '',
    hint: 'The workflow parks on a signal and costs nothing. Send "approve" from the Signal box.',
    code: `async function workflow(ctx) {
  await ctx.activity('createAccount', {})
  const decision = await ctx.waitForSignal('approve')   // parks here
  await ctx.activity('sendWelcomeEmail', { by: decision.by })
  return decision
}`,
  },
  timer: {
    label: 'Durable timer', crash: '',
    hint: 'Kill every worker during the 10 second sleep, wait, then add a new worker: the timer still fires.',
    code: `async function workflow(ctx) {
  await ctx.activity('scheduleReminder', {})
  await ctx.sleep(10000)           // survives every worker dying
  await ctx.activity('sendReminder', {})
}`,
  },
  children: {
    label: 'Child workflows', crash: '',
    hint: 'Two child workflows run in parallel; each has its own history (see the chips above the table).',
    code: `async function workflow(ctx) {
  const results = await Promise.all([
    ctx.child('resize', { file: 'a.png' }),
    ctx.child('resize', { file: 'b.png' }),
  ])
  await ctx.activity('assembleAlbum', { results })
  return results
}

async function resize(ctx, input) {
  await ctx.activity('resizeImage', input)
  return input.file + ' resized'
}`,
  },
};

const SQL_PRESETS: Record<string, string> = {
  'events': `SELECT seq, type, payload->>'name' AS activity, payload->>'worker' AS worker\nFROM events\nWHERE workflow_id = (SELECT id FROM workflows ORDER BY created_at DESC LIMIT 1)\nORDER BY seq;`,
  'workflows': `SELECT id, status, lease_owner, lease_epoch, lease_expires_at, next_seq\nFROM workflows\nORDER BY created_at;`,
  'the lease': `-- who owns each running workflow, and until when?\nSELECT id, lease_owner, lease_epoch,\n       round(extract(epoch FROM (lease_expires_at - now()))::numeric, 1) AS seconds_left\nFROM workflows WHERE status = 'running';`,
  'side effects': `-- the "outside world": one row per real effect\nSELECT idempotency_key, service, protected, ts\nFROM side_effects ORDER BY id;`,
  'duplicates?': `-- any effect that happened more than once?\nSELECT idempotency_key, count(*) AS times\nFROM side_effects GROUP BY 1 HAVING count(*) > 1;`,
  'workers': `SELECT id, status, active, last_seen FROM workers ORDER BY id;`,
};

/* ───────────────────────── state ───────────────────────── */
type Tab = 'history' | 'sql' | 'effects';
let store: StoreBase;
let db: any;
let registry = new Registry();
const workers = new Map<string, Worker>();
const dead = new Set<string>();
const settings = { idempotency: true, speed: 1, armedCrash: '' as string };
let selected: string | null = null;
let counter = 0;
let tab: Tab = 'history';
let booted = false;
let visible = true;
let tick: ReturnType<typeof setInterval> | undefined;
let letterIdx = 0;

/* ───────────────────────── workflow code -> registry ───────────────────────── */
class AutoActivities extends Map<string, ActivityHandler> {
  override get(name: string) { return super.get(name) ?? handlerFor(name); }
  override has() { return true; }
}

function handlerFor(name: string): ActivityHandler {
  return async (input, ac) => {
    await sleep(650 / settings.speed);
    if (/^fail/i.test(name)) throw new NonRetryableError(`${name}: simulated permanent failure`);
    if (/^flaky/i.test(name) && ac.attempt <= 2) throw new Error(`${name}: 503 service unavailable (attempt ${ac.attempt})`);
    // The "outside world". Protected by the idempotency key unless the visitor switched that off.
    const r = await store.pool.query(
      `INSERT INTO side_effects (idempotency_key, service, action, payload, protected, workflow_id)
       VALUES ($1,$2,'run',$3,$4,$5) ON CONFLICT DO NOTHING RETURNING id`,
      [ac.idempotencyKey, name, JSON.stringify(input ?? null), settings.idempotency, ac.workflowId]);
    await store.pool.query(
      `INSERT INTO effect_calls (idempotency_key, service, action, deduped, workflow_id) VALUES ($1,$2,'run',$3,$4)`,
      [ac.idempotencyKey, name, r.rowCount === 0, ac.workflowId]);
    return { ok: true, activity: name, attempt: ac.attempt };
  };
}

function compile(code: string): { ok: true; names: string[] } | { ok: false; error: string } {
  try {
    const names = [...code.matchAll(/async\s+function\s+([A-Za-z_$][\w$]*)\s*\(/g)].map((m) => m[1]);
    if (!names.includes('workflow')) return { ok: false, error: 'Define an `async function workflow(ctx, input)`.' };
    const fns = new Function(`${code}\n;return { ${names.join(', ')} }`)() as Record<string, any>;
    const next = new Registry();
    next.activities = new AutoActivities();
    for (const n of names) next.workflow(n, fns[n]);
    // Hot-swap in place: running workers see the new code on their next replay (change it mid-flight to see non-determinism detected).
    registry.workflows = next.workflows;
    return { ok: true, names };
  } catch (e: any) { return { ok: false, error: String(e?.message ?? e) }; }
}

function activityNames(code: string): string[] {
  return [...new Set([...code.matchAll(/(?:activity|add)\(\s*['"]([^'"]+)['"]/g)].map((m) => m[1]))];
}

/* ───────────────────────── workers ───────────────────────── */
function spawnWorker() {
  const id = String.fromCharCode(65 + (letterIdx++ % 26));
  const w = new Worker({
    id, store, registry, leaseMs: 2500, heartbeatMs: 500, pollMs: 120, maxConcurrent: 3,
    log: (m) => console.debug(m),
    hooks: {
      // The nasty window: the effect has happened, the result has not been recorded.
      afterActivityBeforeRecord: async (name, ac) => {
        if (settings.armedCrash && settings.armedCrash === name) {
          settings.armedCrash = ''; renderArm();
          killWorker(ac.worker);
          await new Promise(() => {});
        }
      },
    },
  });
  workers.set(id, w);
  void w.start();
  return id;
}
function killWorker(id: string) {
  const w = workers.get(id);
  if (!w || dead.has(id)) return;
  w.crash();                       // stops heartbeating and writing: indistinguishable from a dead process
  dead.add(id);
}

/* ───────────────────────── rendering ───────────────────────── */
const last: Record<string, string> = {};
const setHtml = (key: string, el: HTMLElement, html: string) => { if (last[key] !== html) { last[key] = html; el.innerHTML = html; } };

function summarize(e: HistoryEvent): string {
  const p = e.payload as any;
  switch (e.type) {
    case 'WorkflowStarted': return p.workflowType;
    case 'ActivityScheduled': return `#${p.cmd} ${p.name}${p.compensation ? ' (undo)' : ''}`;
    case 'ActivityStarted': return `#${p.cmd} attempt ${p.attempt} · worker ${p.worker}${p.orphan ? ' · re-run after crash' : ''}`;
    case 'ActivityCompleted': return `#${p.cmd} · worker ${p.worker}`;
    case 'ActivityFailed': return `#${p.cmd} attempt ${p.attempt}: ${p.error?.message}${p.final ? ' · gave up' : ' · will retry'}`;
    case 'TimerStarted': return `#${p.cmd} ${p.ms}ms`;
    case 'TimerFired': return `#${p.cmd}`;
    case 'SignalWaitStarted': return `#${p.cmd} waiting for "${p.name}"`;
    case 'SignalReceived': return `"${p.name}" ${JSON.stringify(p.payload)}`;
    case 'SignalMatched': return `#${p.cmd} "${p.name}"`;
    case 'ChildScheduled': return `#${p.cmd} ${p.type} → ${p.childId}`;
    case 'ChildCompleted': case 'ChildFailed': return `#${p.cmd} ${p.childId}`;
    case 'ValueRecorded': return `#${p.cmd} ${p.kind}`;
    case 'CompensationRegistered': return `#${p.cmd} undo with ${p.name}`;
    case 'CompensationStarted': return `#${p.cmd} ${p.count} step(s)`;
    case 'CompensationCompleted': return `#${p.cmd}`;
    case 'WorkerTakeover': return `${p.from} → ${p.to}  (lease epoch ${p.epoch})`;
    case 'WorkflowCompleted': return JSON.stringify(p.result);
    case 'WorkflowFailed': return String(p.error?.message ?? '');
    case 'WorkflowContinuedAsNew': return p.newId;
    default: return '';
  }
}
function tone(e: HistoryEvent): string {
  if (e.type === 'WorkerTakeover') return 'takeover';
  if (e.type === 'WorkflowFailed' || (e.type === 'ActivityFailed' && (e.payload as any).final)) return 'bad';
  if (e.type === 'ActivityFailed' || e.type.startsWith('Compensation')) return 'warn';
  if (/Completed$|Fired$|Matched$/.test(e.type)) return 'ok';
  if (e.type === 'ActivityStarted' && (e.payload as any).orphan) return 'warn';
  return '';
}

function table(rows: any[], cols: string[]): string {
  if (!rows.length) return '<p class="pg-empty">0 rows</p>';
  const cell = (v: any) => {
    if (v === null || v === undefined) return '<i>null</i>';
    if (v instanceof Date) return esc(v.toISOString().slice(11, 23));
    if (typeof v === 'object') return esc(JSON.stringify(v));
    return esc(v);
  };
  return `<div class="pg-table"><table><thead><tr>${cols.map((c) => `<th>${esc(c)}</th>`).join('')}</tr></thead><tbody>${
    rows.slice(0, 200).map((r) => `<tr>${cols.map((c) => `<td>${cell(r[c])}</td>`).join('')}</tr>`).join('')}</tbody></table></div>${
    rows.length > 200 ? `<p class="pg-empty">showing 200 of ${rows.length} rows</p>` : ''}`;
}

async function refresh() {
  if (!booted || !visible) return;
  try {
    const wfs = (await store.pool.query(`SELECT id, status, lease_owner, lease_epoch, lease_expires_at, error, parent_id FROM workflows ORDER BY created_at DESC LIMIT 14`)).rows;
    if (!selected && wfs.length) selected = wfs.find((w: any) => !w.parent_id)?.id ?? wfs[0].id;
    const sel = wfs.find((w: any) => w.id === selected);

    // workflow chips
    setHtml('chips', $('#pgChips'), wfs.map((w: any) =>
      `<button class="pg-chip" data-wf="${esc(w.id)}" data-sel="${w.id === selected}" data-st="${w.status}" title="${esc(w.error?.message ?? '')}">${esc(w.id)} <i>${w.status}</i></button>`).join('')
      || '<span class="pg-empty">no workflows yet: press Start</span>');

    // workers
    const rows = (await store.pool.query(`SELECT id, active, extract(epoch from (now()-last_seen))*1000 AS age FROM workers ORDER BY id`)).rows;
    setHtml('workers', $('#pgWorkers'), [...workers.keys()].map((id) => {
      const isDead = dead.has(id);
      const held = wfs.filter((w: any) => w.lease_owner === id && w.status === 'running');
      const left = held[0]?.lease_expires_at ? Math.max(0, (new Date(held[0].lease_expires_at).getTime() - Date.now()) / 1000) : null;
      const state = isDead ? 'dead' : held.length ? 'busy' : 'idle';
      const note = isDead ? (held.length ? `killed · lease expires in ${left?.toFixed(1)}s` : 'killed') : held.length ? `running ${esc(held[0].id)}` : 'polling for work';
      void rows;
      return `<div class="pg-worker" data-state="${state}"><b>Worker ${id}</b><span>${note}</span>${isDead ? '' : `<button class="pg-kill" data-kill="${id}" type="button">Kill</button>`}</div>`;
    }).join(''));

    // status line for selected workflow
    const status = $('#pgStatus');
    if (sel) {
      const live = workers.size - dead.size;
      const extra = sel.status === 'stuck' ? ` <button class="pg-link" data-unstick="${esc(sel.id)}" type="button">retry</button>`
        : sel.status === 'running' && live === 0 ? ` · <b>no live workers.</b> The workflow is safe in Postgres. <button class="pg-link" data-addworker="1" type="button">Add a worker to resume it</button>` : '';
      setHtml('status', status, `<b>${esc(sel.id)}</b> · ${sel.status}${sel.error ? ' · ' + esc(sel.error.message).slice(0, 140) : ''}${extra}`);
    } else setHtml('status', status, 'Nothing running yet.');

    if (tab === 'history' && sel) {
      const ev = await store.getEvents(sel.id);
      const t0 = new Date(ev[0]?.ts ?? Date.now()).getTime();
      const html = ev.map((e) => `<tr class="${tone(e)}" title="${esc(JSON.stringify(e.payload))}"><td class="n">${e.seq}</td><td class="t">${e.type}</td><td class="d">${esc(summarize(e))}</td><td class="s">+${((new Date(e.ts).getTime() - t0) / 1000).toFixed(1)}s</td></tr>`).join('');
      const box = $('#pgHistory');
      const atBottom = box.scrollTop + box.clientHeight >= box.scrollHeight - 30;
      setHtml('history', $('#pgHistoryBody'), html);
      if (atBottom) box.scrollTop = box.scrollHeight;
    } else if (tab === 'history') setHtml('history', $('#pgHistoryBody'), '');

    if (tab === 'effects') {
      const eff = (await store.pool.query(`SELECT e.id, e.idempotency_key AS key, e.service, e.protected,
          (SELECT count(*)::int FROM side_effects d WHERE d.idempotency_key = e.idempotency_key) AS copies,
          (SELECT count(*)::int FROM effect_calls c WHERE c.idempotency_key = e.idempotency_key) AS calls
        FROM side_effects e ORDER BY e.id DESC LIMIT 60`)).rows;
      const dups = new Set(eff.filter((r: any) => r.copies > 1).map((r: any) => r.key)).size;   // distinct keys that happened more than once
      const ignored = (await store.pool.query(`SELECT count(*)::int AS n FROM effect_calls WHERE deduped`)).rows[0].n;
      setHtml('effects', $('#pgEffectsBody'),
        `<div class="pg-stats"><span><b>${eff.length}</b> effects in the world</span><span class="${dups ? 'bad' : 'good'}"><b>${dups}</b> duplicated</span><span><b>${ignored}</b> retries recognised and ignored</span></div>` +
        (eff.length ? `<div class="pg-table"><table><thead><tr><th>idempotency key</th><th>service</th><th>calls</th><th>copies</th></tr></thead><tbody>${
          eff.map((r: any) => `<tr class="${r.copies > 1 ? 'bad' : ''}"><td>${esc(r.key)}</td><td>${esc(r.service)}</td><td>${r.calls}</td><td>${r.copies}</td></tr>`).join('')}</tbody></table></div>` : '<p class="pg-empty">Nothing has touched the outside world yet.</p>'));
    }
  } catch { /* a transient query error should never break the page */ }
}

function renderArm() {
  const el = $('#pgArmNote');
  el.textContent = settings.armedCrash ? `armed: the first worker to finish "${settings.armedCrash}" will die before recording it` : '';
}
function setBoot(state: 'idle' | 'loading' | 'ready' | 'error', msg = '') {
  $('#pg').dataset.boot = state;
  $('#pgBootMsg').textContent = msg;
  $('#pgPill').textContent = state === 'ready' ? msg : state === 'loading' ? 'booting…' : state === 'error' ? 'failed' : 'not started';
}

/* ───────────────────────── actions ───────────────────────── */
async function bootPg() {
  if (booted) return;
  setBoot('loading', 'Downloading Postgres (WebAssembly)…');
  try {
    const mod: any = await import(/* webpackIgnore: true */ PGLITE_URL);
    setBoot('loading', 'Starting Postgres…');
    db = await mod.PGlite.create();
    store = new StoreBase(new PglitePool(db));
    setBoot('loading', 'Creating the engine tables…');
    await store.migrate();
    const v = String((await db.query('select version()')).rows[0].version).match(/PostgreSQL [\d.]+/)?.[0] ?? 'Postgres';
    registry.activities = new AutoActivities();
    applyPreset($<HTMLSelectElement>('#pgPreset').value);
    booted = true;
    spawnWorker(); spawnWorker();
    setBoot('ready', `${v} · in your browser`);
    tick = setInterval(refresh, 350);
    void refresh();
  } catch (e: any) {
    setBoot('error', `Couldn't start Postgres: ${String(e?.message ?? e)}. Check your connection and try again.`);
  }
}

function applyPreset(key: string) {
  const p = PRESETS[key];
  ($<HTMLTextAreaElement>('#pgCode')).value = p.code;
  $('#pgHint').textContent = p.hint;
  refreshActivityOptions(p.crash);
  compile(p.code);
}
function refreshActivityOptions(preferred?: string) {
  const sel = $<HTMLSelectElement>('#pgCrashAt');
  const names = activityNames($<HTMLTextAreaElement>('#pgCode').value);
  const keep = preferred ?? sel.value;
  sel.innerHTML = `<option value="">nothing (I'll kill one myself)</option>` + names.map((n) => `<option value="${esc(n)}">right after “${esc(n)}” takes effect</option>`).join('');
  sel.value = names.includes(keep) ? keep : '';
}

async function startWorkflow() {
  if (!booted) return;
  const code = $<HTMLTextAreaElement>('#pgCode').value;
  const res = compile(code);
  const err = $('#pgError');
  if (!res.ok) { err.textContent = res.error; err.hidden = false; return; }
  err.hidden = true;
  while (workers.size - dead.size < 2) spawnWorker();     // a crash needs someone to take over
  settings.armedCrash = $<HTMLSelectElement>('#pgCrashAt').value; renderArm();
  const id = `wf-${++counter}`;
  await store.createWorkflow({ id, type: 'workflow', input: {}, codeHash: registry.workflows.get('workflow')?.hash });
  selected = id;
  tab = 'history'; showTab();
  void refresh();
}

async function killHolder() {
  if (!selected) return;
  const r = await store.pool.query(`SELECT lease_owner FROM workflows WHERE id = $1`, [selected]);
  const owner = r.rows[0]?.lease_owner as string | undefined;
  if (owner) killWorker(owner);
  void refresh();
}

async function runSql() {
  const out = $('#pgSqlOut');
  const sql = $<HTMLTextAreaElement>('#pgSql').value.trim();
  if (!sql) return;
  const t0 = performance.now();
  try {
    const res = await store.pool.query(sql);
    const cols = res.rows[0] ? Object.keys(res.rows[0]) : [];
    out.innerHTML = (cols.length ? table(res.rows, cols) : '<p class="pg-empty">OK. 0 rows returned.</p>') +
      `<p class="pg-meta">${res.rows.length} row(s) · ${(performance.now() - t0).toFixed(0)} ms · real Postgres, your private copy</p>`;
  } catch (e: any) { out.innerHTML = `<p class="pg-sqlerr">${esc(e?.message ?? e)}</p>`; }
}

async function resetAll() {
  if (!booted) return;
  for (const [id, w] of workers) { if (!dead.has(id)) w.crash(); }   // just stop them; they are discarded
  workers.clear(); dead.clear(); letterIdx = 0; selected = null; settings.armedCrash = ''; renderArm();
  await store.pool.query('DROP SCHEMA public CASCADE; CREATE SCHEMA public;');
  await store.migrate();
  spawnWorker(); spawnWorker();
  for (const k of Object.keys(last)) delete last[k];
  $('#pgSqlOut').innerHTML = '';
  void refresh();
}

function showTab() {
  document.querySelectorAll<HTMLElement>('[data-tab]').forEach((b) => b.setAttribute('aria-selected', String(b.dataset.tab === tab)));
  document.querySelectorAll<HTMLElement>('[data-panel]').forEach((p) => { p.hidden = p.dataset.panel !== tab; });
  void refresh();
}

/* ───────────────────────── wiring ───────────────────────── */
function init() {
  const root = $('#pg');
  if (!root) return;
  const preset = $<HTMLSelectElement>('#pgPreset');
  preset.innerHTML = Object.entries(PRESETS).map(([k, p]) => `<option value="${k}">${esc(p.label)}</option>`).join('');
  $<HTMLTextAreaElement>('#pgCode').value = PRESETS.checkout.code;
  $('#pgHint').textContent = PRESETS.checkout.hint;
  refreshActivityOptions('chargeCard');
  const sqlBtns = $('#pgSqlPresets');
  sqlBtns.innerHTML = Object.keys(SQL_PRESETS).map((k) => `<button type="button" class="pg-chip" data-sqlp="${esc(k)}">${esc(k)}</button>`).join('');
  $<HTMLTextAreaElement>('#pgSql').value = SQL_PRESETS.events;
  setBoot('idle');

  $('#pgBoot').addEventListener('click', bootPg);
  preset.addEventListener('change', () => applyPreset(preset.value));
  $<HTMLTextAreaElement>('#pgCode').addEventListener('input', () => refreshActivityOptions());
  $<HTMLTextAreaElement>('#pgCode').addEventListener('keydown', (e) => {
    if (e.key !== 'Tab') return;
    e.preventDefault();
    const t = e.target as HTMLTextAreaElement, s = t.selectionStart;
    t.value = t.value.slice(0, s) + '  ' + t.value.slice(t.selectionEnd); t.selectionStart = t.selectionEnd = s + 2;
  });
  $('#pgStart').addEventListener('click', () => void startWorkflow());
  $('#pgKillHolder').addEventListener('click', () => void killHolder());
  $('#pgAddWorker').addEventListener('click', () => { if (booted) { spawnWorker(); void refresh(); } });
  $('#pgReset').addEventListener('click', () => void resetAll());
  $('#pgRunSql').addEventListener('click', () => void runSql());
  $<HTMLInputElement>('#pgIdem').addEventListener('change', (e) => { settings.idempotency = (e.target as HTMLInputElement).checked; });
  $<HTMLSelectElement>('#pgSpeed').addEventListener('change', (e) => { settings.speed = Number((e.target as HTMLSelectElement).value); });
  $<HTMLTextAreaElement>('#pgSql').addEventListener('keydown', (e) => { if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); void runSql(); } });
  $('#pgSendSignal').addEventListener('click', async () => {
    if (!booted || !selected) return;
    let payload: any = null;
    try { payload = JSON.parse($<HTMLInputElement>('#pgSignalPayload').value || 'null'); } catch { payload = $<HTMLInputElement>('#pgSignalPayload').value; }
    await store.signal(selected, $<HTMLInputElement>('#pgSignalName').value || 'approve', payload);
    void refresh();
  });

  root.addEventListener('click', async (e) => {
    const t = (e.target as HTMLElement).closest<HTMLElement>('[data-kill],[data-wf],[data-sqlp],[data-tab],[data-unstick],[data-addworker]');
    if (!t) return;
    if (t.dataset.kill) { killWorker(t.dataset.kill); void refresh(); }
    else if (t.dataset.wf) { selected = t.dataset.wf; void refresh(); }
    else if (t.dataset.sqlp) { $<HTMLTextAreaElement>('#pgSql').value = SQL_PRESETS[t.dataset.sqlp]; void runSql(); }
    else if (t.dataset.tab) { tab = t.dataset.tab as Tab; showTab(); }
    else if (t.dataset.unstick) { await store.unstick(t.dataset.unstick); void refresh(); }
    else if (t.dataset.addworker) { spawnWorker(); void refresh(); }
  });

  if ('IntersectionObserver' in window) new IntersectionObserver((en) => { visible = en[0].isIntersecting; if (visible) void refresh(); }).observe(root);
  showTab();
}

init();
void tick;
