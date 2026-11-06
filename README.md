# durable-flow

A durable execution workflow engine in TypeScript + Postgres. Write multi-step processes as plain async functions;
if the worker running one dies halfway, another replays the event history and carries on, with no duplicate side effects.

**Website and live playground: https://durable-flow.vercel.app** (the playground runs this exact engine on a real
Postgres, compiled to WebAssembly, inside your browser: kill workers, edit the workflow, query the tables).

```ts
const registry = new Registry()
  .workflow('checkout', async (ctx, order) => {
    await ctx.activity('reserveStock', order)
    await ctx.activity('chargeCard', order)      // recorded, retried, given an idempotency key
    await ctx.sleep(24 * 60 * 60 * 1000)         // a durable timer: holds no worker
    await ctx.activity('sendReceipt', order)
  })
```

## Try it (about two minutes)

Needs Node.js 20+ (developed on 24) and git. No Docker and no Postgres install: it downloads a throwaway Postgres.

```sh
git clone https://github.com/karmic05/durable-flow.git
cd durable-flow
npm install
npm run quickstart   # kills a worker right after it charges a card; another finishes the job; charged once
npm test             # 28 tests
```

## What it does

Deterministic replay with non-determinism detection · fenced leases and crash recovery · durable timers · retries with
backoff · sagas (compensation) · signals · child workflows · `continueAsNew` · `ctx.patched()` versioning ·
determinism fuzzer · OpenTelemetry spans · runs on `pg` (Node) or PGlite (browser).

## What it is not

- **Not exactly-once execution.** Activities run *at least once*. If a worker dies after a side effect but before the
  result is recorded, the activity runs again. You get exactly-once *effects* only if the service you call honours the
  idempotency key (`ac.idempotencyKey`, stable across retries and takeovers). `npm run quickstart` shows both sides.
- **Not a Temporal replacement.** One Postgres, polling workers, TypeScript only. No sharding, multi-region,
  multi-tenancy, auth, or production hardening. It is a working implementation of the idea, built to learn from.
- **Your workflow code must be deterministic.** No `Date.now()`, `Math.random()` or network calls; use `ctx.now()`,
  `ctx.random()` and activities.
- **Not on npm yet.** Use it from this repo (add workflows next to `examples/`) or copy `packages/engine/src`.

## Layout

```
packages/engine   the engine: replay, store, worker, leases, tracing, fuzzer, PGlite adapter
examples/         order-saga workflows and `npm run quickstart`
apps/landing      the website + in-browser playground (static; deployed to Vercel)
apps/server       Fastify API / SSE / worker supervisor (written, not yet finished or deployed)
```

## Status

Engine, tests, quickstart, website and playground are done and working. The React dashboard (history timeline, state
inspector, chaos mode) and a hosted server are not finished.

## Working on the website

`apps/landing/src/playground.ts` is bundled into the committed `apps/landing/playground.js`:

```sh
npm run build:landing        # after editing the playground
cd apps/landing && vercel deploy --prod
```
