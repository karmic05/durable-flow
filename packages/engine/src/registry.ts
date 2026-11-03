import { createHash } from 'node:crypto';
import type { Store } from './store.js';
import type { WorkflowFn } from './replay.js';

export interface ActivityContext {
  workflowId: string;
  cmd: number;
  attempt: number;
  /** Stable across retries AND worker takeovers. Pass it to external systems so they can dedupe. */
  idempotencyKey: string;
  worker: string;
  store: Store;
  deps: any;
}
export type ActivityHandler = (input: any, ac: ActivityContext) => Promise<any>;

export interface WorkflowDefinition { type: string; fn: WorkflowFn; hash: string }

export const hashFn = (fn: Function) => createHash('sha1').update(fn.toString()).digest('hex').slice(0, 8);

export class Registry {
  workflows = new Map<string, WorkflowDefinition>();
  activities = new Map<string, ActivityHandler>();
  workflow(type: string, fn: WorkflowFn) { this.workflows.set(type, { type, fn, hash: hashFn(fn) }); return this; }
  activity(name: string, handler: ActivityHandler) { this.activities.set(name, handler); return this; }
}
