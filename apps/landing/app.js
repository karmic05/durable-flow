(() => {
  'use strict';
  const $ = (s, r = document) => r.querySelector(s);
  const $$ = (s, r = document) => [...r.querySelectorAll(s)];
  const reduceMotion = matchMedia('(prefers-reduced-motion: reduce)').matches;

  /* ───────── scroll reveal ───────── */
  const reveals = $$('.reveal');
  if ('IntersectionObserver' in window) {
    const io = new IntersectionObserver((entries) => {
      entries.forEach((e) => { if (e.isIntersecting) { e.target.classList.add('in'); io.unobserve(e.target); } });
    }, { rootMargin: '0px 0px -6% 0px', threshold: 0.06 });
    reveals.forEach((el) => io.observe(el));
  } else reveals.forEach((el) => el.classList.add('in'));

  /* ───────── copy buttons ───────── */
  $$('.copy').forEach((btn) => btn.addEventListener('click', async () => {
    const text = btn.dataset.copy || '';
    try { await navigator.clipboard.writeText(text); }
    catch {
      const ta = document.createElement('textarea'); ta.value = text; ta.style.position = 'fixed'; ta.style.opacity = '0';
      document.body.appendChild(ta); ta.select(); try { document.execCommand('copy'); } catch { /* ignore */ } ta.remove();
    }
    btn.dataset.state = 'done'; clearTimeout(btn._t); btn._t = setTimeout(() => { btn.dataset.state = ''; }, 1500);
  }));

  /* ───────── crash & resume simulation ─────────
     You decide when Worker A dies. Each step has a "work" phase, then the side effect lands in the outside world,
     then a short gap before the engine records the result (amber on the bar). Kill A inside the gap and the
     effect has happened but the history doesn't know. Worker B must re-run the step. */
  const demo = $('#demo');
  if (!demo) return;

  const STEPS = [
    { k: 'reserve', svc: 'inventory', label: 'inventory.reserve', name: 'reserveInventory', work: 700, gap: 400 },
    { k: 'charge', svc: 'payments', label: 'payments.charge', name: 'chargeCard', work: 850, gap: 500 },
    { k: 'review', svc: null, name: 'sleep(4s)', work: 1200, gap: 0, timer: true },
    { k: 'ship', svc: 'carrier', label: 'carrier.dispatch', name: 'child: fulfillment', work: 700, gap: 400 },
    { k: 'email', svc: 'email', label: 'email.send', name: 'sendConfirmation', work: 520, gap: 340 },
  ];
  const KILLED = Symbol('killed');
  const ABORT = Symbol('abort');

  const runBtn = $('#run'), manualBtn = $('#runManual'), killBtn = $('#kill'), idem = $('#idem');
  const logEl = $('#log'), verdict = $('#verdict'), dedupedEl = $('#deduped');
  const A = $('#wA'), B = $('#wB');
  const rows = Object.fromEntries($$('#steps li').map((li) => [li.dataset.step, li]));
  const counts = Object.fromEntries($$('.ledger-row').map((r) => [r.dataset.svc, $('b', r)]));
  const scaled = (ms) => (reduceMotion ? ms * 0.6 : ms);

  let S;          // per-run state
  let hintTimer;

  /* cancellable sleep: kill() rejects every pending wait at once */
  const pending = new Set();
  const sleep = (ms) => new Promise((res, rej) => {
    const e = { rej }; e.t = setTimeout(() => { pending.delete(e); res(); }, scaled(ms)); pending.add(e);
  });
  const cancelAll = (reason) => { pending.forEach((e) => { clearTimeout(e.t); e.rej(reason); }); pending.clear(); };

  /* ── bars ── */
  const fills = (k) => $$('.bar i', rows[k]);
  const setFill = (el, p, ms) => { el.style.setProperty('--d', ms + 'ms'); void el.offsetWidth; el.style.setProperty('--p', p); };
  const resetBar = (k) => fills(k).forEach((i) => setFill(i, 0, 0));
  const freezeBar = (k) => fills(k).forEach((i) => {
    const m = getComputedStyle(i, '::after').transform.match(/matrix\(([^,]+)/);
    setFill(i, m ? parseFloat(m[1]) : 0, 0);
  });

  /* ── ui helpers ── */
  const setStep = (k, state, tag = '') => {
    rows[k].dataset.s = state; $('.tag', rows[k]).textContent = tag;
    if (state === 'pending') resetBar(k);
  };
  const setWorker = (w, state, pill, note) => {
    w.dataset.state = state; $('[data-role=pill]', w).textContent = pill;
    if (note != null) $('[data-role=note]', w).textContent = note;
  };
  const setLease = (w, scale, ms = 0) => {
    const i = $('[data-role=lease]', w);
    i.style.transition = ms ? `transform ${scaled(ms)}ms linear` : 'none';
    void i.offsetWidth;
    i.style.transform = `scaleX(${scale})`;
  };
  const setCount = (svc, n, tone) => {
    const b = counts[svc]; b.textContent = n; b.className = tone || '';
    if (!reduceMotion && n) { b.classList.add('bump'); setTimeout(() => b.classList.remove('bump'), 220); }
  };
  let seq = 0;
  const log = (text, cls = '', replayed = false) => {
    const li = document.createElement('li');
    li.className = [cls, replayed ? 'replayed' : ''].join(' ').trim();
    li.innerHTML = `<span class="n">${replayed ? '↻' : ++seq}</span><span class="t"></span>`;
    $('.t', li).textContent = text; li.title = text;
    logEl.appendChild(li);
    while (logEl.children.length > 16) logEl.firstElementChild.remove();
  };
  const say = (html, tone) => { verdict.innerHTML = `<span>${html}</span>`; verdict.dataset.tone = tone; };
  const syncButtons = () => {
    runBtn.disabled = S.running; manualBtn.disabled = S.running;
    killBtn.disabled = !(S.running && !S.killed && S.worker === 'A');
    idem.disabled = S.running;
    if (killBtn.disabled) { killBtn.dataset.hint = '0'; clearTimeout(hintTimer); }
  };

  function reset() {
    cancelAll(ABORT);
    seq = 0; logEl.innerHTML = '';
    STEPS.forEach((s) => setStep(s.k, 'pending'));
    [A, B].forEach((w) => { setWorker(w, 'idle', 'idle', 'waiting for work'); setLease(w, 0); });
    Object.keys(counts).forEach((k) => setCount(k, 0));
    dedupedEl.textContent = '0'; dedupedEl.className = '';
    S = { running: false, killed: false, worker: 'A', recorded: new Set(), applied: new Set(), scheduled: new Set(), cur: null, timerStart: 0, effects: {}, safe: true, limbo: null };
    demo.dataset.run = 'idle';
    syncButtons();
  }

  /* ── one step on one worker ── */
  async function execStep(step, who, resumed) {
    const n = STEPS.indexOf(step) + 1;
    const [f1, f2] = fills(step.k);
    S.cur = step;
    setStep(step.k, 'running', who + (resumed ? ' · retry' : ''));
    resetBar(step.k);

    if (!S.scheduled.has(step.k)) { S.scheduled.add(step.k); log(`${step.timer ? 'TimerStarted' : 'ActivityScheduled'}  #${n} ${step.name}`); }
    if (!step.timer) log(`ActivityStarted  #${n} (${who}${resumed ? ', resumed' : ''})`, resumed ? 'warn' : '');

    let work = step.work;
    if (step.timer) {
      if (resumed) {                      // the timer is durable: B only waits out the remainder
        const done = Math.min(step.work - 300, S.timerStart);
        work = step.work - done; setFill(f1, done / step.work, 0);
        log('timer persisted in history, waiting the remainder', 'ok');
      } else S.timerStart = 0;
    }
    const t0 = performance.now();
    setFill(f1, 1, scaled(work));
    try { await sleep(work); } finally { if (step.timer) S.timerStart = Math.max(S.timerStart, (performance.now() - t0) / (reduceMotion ? 0.6 : 1) + (step.work - work)); }

    if (step.svc) {                        // the effect lands in the outside world
      if (S.applied.has(step.k)) {         // …again, because the first run's result was never recorded
        if (S.safe) { dedupedEl.textContent = '1'; dedupedEl.className = 'good'; log(`${step.label}: key seen, ignored`, 'ok'); }
        else { S.effects[step.svc]++; setCount(step.svc, S.effects[step.svc], 'bad'); log(`${step.label} applied AGAIN`, 'bad'); }
      } else { S.applied.add(step.k); S.effects[step.svc] = 1; setCount(step.svc, 1, 'good'); }
      setFill(f2, 1, scaled(step.gap));
      if (S.auto && step.k === 'charge' && !S.killed) setTimeout(killNow, scaled(240));   // scripted crash, inside the danger window
      await sleep(step.gap);
    }
    log(`${step.timer ? 'TimerFired' : 'ActivityCompleted'}  #${n}`, 'ok');
    S.recorded.add(step.k);
    setStep(step.k, 'done', who);
    fills(step.k).forEach((i) => setFill(i, 1, 0));
  }

  async function runWorker(who) {
    for (const step of STEPS) {
      const n = STEPS.indexOf(step) + 1;
      if (S.recorded.has(step.k)) {
        if (who === 'B') { setStep(step.k, 'replayed', 'from history'); log(`#${n} ${step.name} → skipped`, '', true); await sleep(180); }
        continue;
      }
      await execStep(step, who, who === 'B' && S.scheduled.has(step.k));
    }
  }

  /* ── scenarios ── */
  async function start(auto) {
    reset();
    S.running = true; S.safe = idem.checked; S.auto = !!auto;
    demo.dataset.run = 'running';
    syncButtons();
    hintTimer = setTimeout(() => { if (!killBtn.disabled) killBtn.dataset.hint = '1'; }, 1400);

    say(auto ? 'Worker A is executing. It is about to be killed at the worst possible moment.' : 'Worker A is executing. Press <b>Kill worker A</b> whenever you like, ideally while a bar is amber.', 'idle');
    setWorker(A, 'running', 'running', 'claimed · lease epoch 1'); setLease(A, 1);
    log('WorkflowStarted  orderSaga');
    try {
      await sleep(350);
      await runWorker('A');
      setWorker(A, 'idle', 'done', 'finished · lease released'); setLease(A, 0, 300);
      log('WorkflowCompleted', 'ok');
      S.running = false; demo.dataset.run = 'good'; runBtn.textContent = 'Run & crash it again';
      say('<b>No crash this time.</b> It ran start to finish on Worker A. Run it again and kill A while a step\'s bar is amber.', 'idle');
      syncButtons();
    } catch (e) {
      if (e === KILLED) await takeover();
      else if (e !== ABORT) throw e;
    }
  }

  async function takeover() {
    const step = S.cur ?? STEPS[0];
    freezeBar(step.k);
    const inWindow = !!(step.svc && S.applied.has(step.k) && !S.recorded.has(step.k));
    const limbo = inWindow ? step : null;      // the step whose effect happened but was never recorded
    S.limbo = limbo;

    setWorker(A, 'dead', 'SIGKILL', 'process gone, nothing written');
    log('✕ Worker A killed (SIGKILL)', 'bad');
    if (inWindow) {
      setStep(step.k, 'limbo', 'effect applied, not recorded');
      say(`<b>Worker A is dead mid-flight.</b> <code>${step.label}</code> already happened, but the history never heard about it. This is the window that breaks naive retries.`, 'warn');
    } else {
      setStep(step.k, 'pending', 'interrupted');
      say(step.svc ? '<b>Worker A is dead.</b> It died before the effect landed, so there is nothing to undo. Watch B pick it up.'
                   : '<b>Worker A is dead during a durable timer.</b> The timer lives in history, not in A. Watch B pick it up.', 'warn');
    }
    syncButtons();

    await sleep(500);
    setWorker(B, 'idle', 'polling', 'watching for expired leases');
    setWorker(A, 'dead', 'SIGKILL', 'lease expiring…'); setLease(A, 0, 1900);
    await sleep(2000);
    log('lease expired  (epoch 1, owner A)', 'warn');
    await sleep(250);

    S.worker = 'B'; syncButtons();
    setWorker(B, 'replaying', 'replaying', 'claimed · lease epoch 2'); setLease(B, 1);
    setWorker(A, 'dead', 'SIGKILL', 'fenced out at epoch 2');
    log('WorkerTakeover  A → B  (epoch 2)', 'ok');
    say('<b>Worker B claims the workflow</b> and re-runs the function against history. Recorded steps return instantly.', 'idle');
    await sleep(450);
    setWorker(B, 'running', 'running', 'resuming from history');
    await runWorker('B');
    setWorker(B, 'idle', 'done', 'finished · lease released'); setLease(B, 0, 300);
    log('WorkflowCompleted', 'ok');

    S.running = false; runBtn.textContent = 'Run & crash it again'; syncButtons();
    const skipped = [...S.recorded].length;
    if (limbo && S.safe) {
      demo.dataset.run = 'good';
      say(`<b>No duplicate side effects.</b> B re-ran <code>${limbo.label}</code> with the same idempotency key and the service ignored it. Now switch the keys off and do it again.`, 'good');
    } else if (limbo) {
      demo.dataset.run = 'bad';
      say(`<b>Duplicate side effect: <code>${limbo.label}</code> ran twice.</b> The engine replayed perfectly; the call just wasn't idempotent. That's what the key is for.`, 'bad');
    } else {
      demo.dataset.run = 'good';
      say(`<b>Clean recovery.</b> Nothing was half-done, so B skipped what history had and carried on (${skipped} steps recorded). Try again and kill A while a bar is amber.`, 'good');
    }
  }

  function killNow() {
    if (killBtn.disabled) return;
    S.killed = true; clearTimeout(hintTimer); killBtn.dataset.hint = '0';
    syncButtons();
    cancelAll(KILLED);
  }

  runBtn.addEventListener('click', () => start(true));
  manualBtn.addEventListener('click', () => start(false));
  killBtn.addEventListener('click', killNow);
  addEventListener('keydown', (e) => {
    if ((e.key === 'k' || e.key === 'K') && !e.metaKey && !e.ctrlKey && !e.altKey && !/input|textarea/i.test(e.target.tagName)) killNow();
  });
  reset();
})();
