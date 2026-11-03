// Runs in a worker_thread so the fuzzer can swap Date/Math/etc. without touching the server's own globals.
import { parentPort, workerData } from 'node:worker_threads';
import { fuzzReplay } from '@durable-flow/engine';
import { buildRegistry } from '@durable-flow/examples';

const def = buildRegistry().workflows.get(workerData.type);
if (!def) parentPort!.postMessage({ error: `unknown workflow type ${workerData.type}` });
else parentPort!.postMessage({ report: await fuzzReplay(def.fn, workerData.events, workerData.runs ?? 25) });
