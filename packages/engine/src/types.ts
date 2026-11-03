export type EventType =
  | 'WorkflowStarted' | 'WorkflowCompleted' | 'WorkflowFailed' | 'WorkflowContinuedAsNew'
  | 'ActivityScheduled' | 'ActivityStarted' | 'ActivityCompleted' | 'ActivityFailed'
  | 'TimerStarted' | 'TimerFired'
  | 'SignalReceived' | 'SignalWaitStarted' | 'SignalMatched'
  | 'ValueRecorded' | 'PatchMarker'
  | 'ChildScheduled' | 'ChildCompleted' | 'ChildFailed'
  | 'CompensationRegistered' | 'CompensationStarted' | 'CompensationCompleted'
  | 'WorkerTakeover';

export interface HistoryEvent {
  seq: number;
  type: EventType;
  payload: Record<string, any>;
  ts: string; // ISO
}
export type EventDraft = { type: EventType; payload: Record<string, any> };

export interface RetryPolicy {
  maxAttempts: number;          // total attempts (1 = no retry)
  initialIntervalMs: number;
  backoff: number;              // multiplier
  maxIntervalMs: number;
  jitter: number;               // 0..1 fraction
  nonRetryable: string[];       // error names that are never retried
}
export const DEFAULT_RETRY: RetryPolicy = {
  maxAttempts: 4, initialIntervalMs: 500, backoff: 2, maxIntervalMs: 10_000, jitter: 0.1, nonRetryable: [],
};

export interface ActivityOptions {
  retry?: Partial<RetryPolicy>;
  timeoutMs?: number;
}

export interface SerializedError { name: string; message: string; stack?: string }

export type WorkflowStatus = 'running' | 'completed' | 'failed' | 'stuck';

export interface WorkflowRow {
  id: string;
  type: string;
  status: WorkflowStatus;
  input: any;
  result: any;
  error: SerializedError | null;
  lease_owner: string | null;
  lease_expires_at: string | null;
  lease_epoch: number;
  next_wake_at: string;
  next_seq: number;
  parent_id: string | null;
  parent_cmd: number | null;
  root_id: string;
  code_hash: string | null;
  created_at: string;
  updated_at: string;
}

export class NonRetryableError extends Error {
  constructor(message: string) { super(message); this.name = 'NonRetryableError'; }
}
export class NonDeterminismError extends Error {
  constructor(message: string) { super(message); this.name = 'NonDeterminismError'; }
}
export class LeaseLostError extends Error {
  constructor(msg = 'lease lost (fenced out by a newer epoch)') { super(msg); this.name = 'LeaseLostError'; }
}
/** Thrown inside workflow code when an activity fails permanently. */
export class ActivityFailure extends Error {
  constructor(public activity: string, public attempts: number, public cause_: SerializedError) {
    super(`activity "${activity}" failed after ${attempts} attempt(s): ${cause_.message}`);
    this.name = 'ActivityFailure';
  }
}
export class ChildWorkflowFailure extends Error {
  constructor(public childType: string, public cause_: SerializedError) {
    super(`child workflow "${childType}" failed: ${cause_.message}`);
    this.name = 'ChildWorkflowFailure';
  }
}
export class ContinueAsNewSignal extends Error {
  constructor(public input: any) { super('continue-as-new'); this.name = 'ContinueAsNewSignal'; }
}
