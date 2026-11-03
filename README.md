# durable-flow

A durable execution workflow engine in TypeScript + Postgres: event-sourced replay, durable timers, retries, sagas,
signals, child workflows, continue-as-new, `ctx.patched()` versioning, a determinism fuzzer and OpenTelemetry traces.

**Status: work in progress.** Engine core and tests are done (`npm test`); server API is written; React dashboard and
landing page are in progress. Full README (architecture, the exactly-once caveat, deploy guide) comes with the first release.
