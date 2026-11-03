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
    }, { rootMargin: '0px 0px -8% 0px', threshold: 0.08 });
    reveals.forEach((el) => io.observe(el));
  } else reveals.forEach((el) => el.classList.add('in'));

  /* ───────── crash & resume simulation ───────── */
  const demo = $('#demo');
  if (!demo) return;
  const runBtn = $('#run');
  const idem = $('#idem');
  const logEl = $('#log');
  const verdict = $('#verdict');
  const dedupedEl = $('#deduped');
  const A = $('#wA'), B = $('#wB');
  const steps = Object.fromEntries($$('#steps li').map((li) => [li.dataset.step, li]));
  const ledger = { inventory: $('[data-svc=inventory] b'), payments: $('[data-svc=payments] b') };

  let runId = 0;
  const ABORT = Symbol('abort');
  const wait = (ms, id) => new Promise((res, rej) => setTimeout(() => (id === runId ? res() : rej(ABORT)), reduceMotion ? ms * 0.6 : ms));

  const setStep = (k, state, tag = '') => { steps[k].dataset.s = state; steps[k].querySelector('.tag').textContent = tag; };
  const setWorker = (w, state, pill, note) => {
    w.dataset.state = state;
    w.querySelector('[data-role=pill]').textContent = pill;
    if (note != null) w.querySelector('[data-role=note]').textContent = note;
  };
  const setLease = (w, scale, ms = 0) => {
    const i = w.querySelector('[data-role=lease]');
    i.style.transition = ms ? `transform ${ms}ms linear` : 'none';
    // force style flush so a following transition starts from the value we just set
    void i.offsetWidth;
    i.style.transform = `scaleX(${scale})`;
  };
  const setCount = (svc, n, tone) => {
    const b = ledger[svc];
    b.textContent = n;
    b.className = tone || '';
    if (!reduceMotion) { b.classList.add('bump'); setTimeout(() => b.classList.remove('bump'), 220); }
  };

  let seq = 0;
  const log = (text, cls = '', replayed = false) => {
    const li = document.createElement('li');
    li.className = [cls, replayed ? 'replayed' : ''].join(' ').trim();
    li.innerHTML = `<span class="n">${replayed ? '↻' : ++seq}</span><span class="t"></span>`;
    li.querySelector('.t').textContent = text;
    li.title = text;
    logEl.appendChild(li);
    while (logEl.children.length > 14) logEl.firstElementChild.remove();
  };
  const say = (html, tone) => { verdict.innerHTML = html; verdict.dataset.tone = tone; };

  function reset() {
    seq = 0; logEl.innerHTML = '';
    Object.keys(steps).forEach((k) => setStep(k, 'pending'));
    [A, B].forEach((w) => { setWorker(w, 'idle', 'idle', 'waiting for work'); setLease(w, 0); });
    setCount('inventory', 0); setCount('payments', 0); dedupedEl.textContent = '0';
    demo.dataset.run = 'idle';
  }

  async function run() {
    const id = ++runId;
    const safe = idem.checked;
    reset();
    demo.dataset.run = 'running';
    runBtn.disabled = true; idem.disabled = true; runBtn.textContent = 'Running…';
    const w = (ms) => wait(ms, id);
    try {
      say('Worker A claims the workflow and starts executing steps.', 'idle');
      setWorker(A, 'running', 'running', 'claimed · lease epoch 1'); setLease(A, 1);
      log('WorkflowStarted  orderSaga', '');
      await w(450);

      // 1 ── reserve
      setStep('reserve', 'running', 'A');
      log('ActivityScheduled  #1 reserveInventory');
      log('ActivityStarted    #1 attempt 1  (A)');
      await w(750);
      setCount('inventory', 1, 'good');
      log('ActivityCompleted  #1', 'ok');
      setStep('reserve', 'done', 'A');
      await w(250);

      // 2 ── charge, then crash in the dangerous window
      setStep('charge', 'running', 'A');
      log('ActivityScheduled  #2 chargeCard');
      log('ActivityStarted    #2 attempt 1  (A)');
      await w(800);
      setCount('payments', 1, 'good');
      setWorker(A, 'running', 'running', 'payments.charge applied…');
      await w(380);

      setWorker(A, 'dead', 'SIGKILL', 'process gone. nothing was written.');
      setStep('charge', 'limbo', 'charged, not recorded');
      log('✕ worker A killed (SIGKILL)', 'bad');
      say('<b>Worker A is dead.</b> The card <i>has</i> been charged, but the history never heard about it. This is the window that breaks naive retry logic.', 'warn');
      await w(500);

      // lease drains
      setWorker(B, 'idle', 'polling', 'watching for expired leases');
      setLease(A, 0, 1900);
      setWorker(A, 'dead', 'SIGKILL', 'lease expiring…');
      await w(2000);
      log('lease expired  (epoch 1, owner A)', 'warn');
      await w(250);

      // 3 ── B takes over and replays
      setWorker(B, 'replaying', 'replaying', 'claimed · lease epoch 2'); setLease(B, 1);
      log('WorkerTakeover  A → B  (epoch 2)', 'ok');
      say('<b>Worker B claims the workflow</b> and re-runs the function against history. Finished steps return instantly from the log.', 'idle');
      await w(500);
      setStep('reserve', 'replayed', 'from history');
      log('#1 reserveInventory → skipped', '', true);
      await w(900);

      setWorker(B, 'running', 'running', 'resuming at command #2');
      setStep('charge', 'running', 'B · retry');
      log('ActivityStarted    #2 attempt 1  (B, resumed)', 'warn');
      await w(850);
      if (safe) {
        dedupedEl.textContent = '1'; dedupedEl.className = 'good';
        log('payments.charge: key seen, ignored', 'ok');
      } else {
        setCount('payments', 2, 'bad');
        log('payments.charge applied AGAIN', 'bad');
      }
      log('ActivityCompleted  #2', 'ok');
      setStep('charge', 'done', 'B');
      await w(300);

      // 4 ── the rest
      setStep('review', 'running', 'timer');
      log('TimerStarted  #3  4s');
      await w(750);
      log('TimerFired    #3', 'ok'); setStep('review', 'done', 'B');
      setStep('ship', 'running', 'child');
      log('ChildScheduled  #4 fulfillment');
      await w(700);
      log('ChildCompleted  #4', 'ok'); setStep('ship', 'done', 'B');
      setStep('email', 'running', 'B');
      log('ActivityScheduled  #5 sendConfirmationEmail');
      await w(550);
      log('ActivityCompleted  #5', 'ok'); setStep('email', 'done', 'B');
      log('WorkflowCompleted', 'ok');
      setWorker(B, 'idle', 'done', 'finished · lease released'); setLease(B, 0, 400);

      if (safe) {
        say('<b>5 steps, 1 crash, 0 duplicate effects.</b> The retried charge carried the same idempotency key, so the payment service recognised it and did nothing. Now flip the switch off and run it again.', 'good');
        demo.dataset.run = 'done';
      } else {
        say('<b>The customer was charged twice.</b> The engine replayed perfectly; the payment call just wasn\'t idempotent. That is why the key exists, and why "exactly-once" really means at-least-once plus idempotency.', 'bad');
        demo.dataset.run = 'bad';
      }
    } catch (e) {
      if (e !== ABORT) throw e;
      return;
    }
    runBtn.disabled = false; idem.disabled = false; runBtn.textContent = 'Run again';
  }

  runBtn.addEventListener('click', run);
  reset();
})();
