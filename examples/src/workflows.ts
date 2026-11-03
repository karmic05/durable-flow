import type { WorkflowContext } from '@durable-flow/engine';

export interface OrderInput {
  orderId: string;
  amount: number;
  items: string[];
  /** Force a step to fail permanently, to demonstrate saga compensation. */
  failAt?: 'charge' | 'ship' | null;
  /** Wait for a human `approve` signal instead of a timed fraud review. */
  approval?: boolean;
  fraudReviewMs?: number;
}

/**
 * Order fulfilment saga. Plain async TypeScript: every `ctx.*` call is recorded in the event history, so after a
 * crash another worker re-runs this function, skips everything already done, and continues from the same point.
 */
export async function orderSaga(ctx: WorkflowContext, input: OrderInput) {
  const state = { step: 'starting', orderId: input.orderId, trackingId: null as string | null };
  ctx.setQueryHandler('status', () => state);

  const saga = ctx.saga();
  try {
    state.step = 'reserving inventory';
    await ctx.activity('reserveInventory', { orderId: input.orderId, items: input.items });
    saga.add('releaseInventory', { orderId: input.orderId });

    state.step = 'charging card';
    await ctx.activity('chargeCard', { orderId: input.orderId, amount: input.amount, fail: input.failAt === 'charge' },
      { retry: { maxAttempts: 3, initialIntervalMs: 400, backoff: 2 } });
    saga.add('refundCard', { orderId: input.orderId, amount: input.amount });

    if (input.approval) {
      state.step = 'awaiting manager approval';
      const decision = await ctx.waitForSignal<{ approved: boolean; by?: string }>('approve');
      if (!decision.approved) throw new Error(`order rejected by ${decision.by ?? 'reviewer'}`);
    } else {
      state.step = 'fraud review (durable timer)';
      await ctx.sleep(input.fraudReviewMs ?? 4000);
    }

    state.step = 'fulfilment';
    const shipment = await ctx.child<{ trackingId: string }>('fulfillment', { orderId: input.orderId, fail: input.failAt === 'ship' });
    state.trackingId = shipment.trackingId;
    saga.add('cancelShipment', { orderId: input.orderId, trackingId: shipment.trackingId });

    state.step = 'sending confirmation';
    await ctx.activity('sendConfirmationEmail', { orderId: input.orderId, trackingId: shipment.trackingId });

    state.step = 'completed';
    return { orderId: input.orderId, trackingId: shipment.trackingId, charged: input.amount };
  } catch (err) {
    state.step = 'compensating';
    await saga.compensate();
    state.step = 'rolled back';
    throw err;
  }
}

/** Child workflow: pack the order and hand it to a carrier. */
export async function fulfillment(ctx: WorkflowContext, input: { orderId: string; fail?: boolean }) {
  await ctx.activity('packOrder', { orderId: input.orderId });
  const trackingId = 'TRK-' + ctx.uuid().slice(0, 8).toUpperCase();   // recorded once, identical on every replay
  await ctx.activity('dispatchCarrier', { orderId: input.orderId, trackingId, fail: !!input.fail },
    { retry: { maxAttempts: 2, initialIntervalMs: 300 } });
  return { trackingId };
}

/**
 * Long-running loop that keeps its history bounded with continue-as-new: after `perRun` iterations it hands
 * its state to a fresh execution with an empty history.
 */
export async function tickLoop(ctx: WorkflowContext, input: { done: number; total: number; perRun?: number }) {
  const perRun = input.perRun ?? 5;
  let done = input.done;
  for (let i = 0; i < perRun && done < input.total; i++) {
    await ctx.activity('tick', { n: done });
    done++;
  }
  if (done < input.total) ctx.continueAsNew({ done, total: input.total, perRun });
  return { ticks: done };
}

/** Retry demo: the activity fails `failures` times and then succeeds; every attempt is in the history. */
export async function retryDemo(ctx: WorkflowContext, input: { failures: number }) {
  const out = await ctx.activity<string>('flakyApi', { failures: input.failures },
    { retry: { maxAttempts: input.failures + 2, initialIntervalMs: 500, backoff: 2, jitter: 0 } });
  return { out };
}
