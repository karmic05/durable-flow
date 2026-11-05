import type { StoreBase } from './store-base.js';
import type { WorkflowFn } from './replay.js';

export interface ActivityContext {
  workflowId: string;
  cmd: number;
  attempt: number;
  /** Stable across retries AND worker takeovers. Pass it to external systems so they can dedupe. */
  idempotencyKey: string;
  worker: string;
  store: StoreBase;
  deps: any;
}
export type ActivityHandler = (input: any, ac: ActivityContext) => Promise<any>;

export interface WorkflowDefinition { type: string; fn: WorkflowFn; hash: string }

/** 32-bit FNV-1a of the function source: a cheap fingerprint to notice that workflow code changed. */
export const hashFn = (fn: Function) => {
  let h = 0x811c9dc5; const s = fn.toString();
  for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193); }
  return (h >>> 0).toString(16).padStart(8, '0');
};

export class Registry {
  workflows = new Map<string, WorkflowDefinition>();
  activities = new Map<string, ActivityHandler>();
  workflow(type: string, fn: WorkflowFn) { this.workflows.set(type, { type, fn, hash: hashFn(fn) }); return this; }
  activity(name: string, handler: ActivityHandler) { this.activities.set(name, handler); return this; }
}
