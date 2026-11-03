import { NonRetryableError, Registry } from '@durable-flow/engine';
import { callService } from './services.js';
import { fulfillment, orderSaga, retryDemo, tickLoop } from './workflows.js';

export * from './workflows.js';
export * from './services.js';

export function buildRegistry(): Registry {
  const r = new Registry();
  r.workflow('orderSaga', orderSaga);
  r.workflow('fulfillment', fulfillment);
  r.workflow('tickLoop', tickLoop);
  r.workflow('retryDemo', retryDemo);

  r.activity('reserveInventory', async (i, ac) => {
    await callService(ac, 'inventory', 'reserve', i, 800);
    return { reservationId: `RSV-${i.orderId}` };
  });
  r.activity('releaseInventory', async (i, ac) => { await callService(ac, 'inventory', 'release', i, 600); });
  r.activity('chargeCard', async (i, ac) => {
    if (i.fail) throw new NonRetryableError('card declined (insufficient funds)');
    await callService(ac, 'payments', 'charge', { orderId: i.orderId, amount: i.amount }, 1100);
    return { chargeId: `CH-${i.orderId}` };
  });
  r.activity('refundCard', async (i, ac) => { await callService(ac, 'payments', 'refund', i, 700); });
  r.activity('packOrder', async (i, ac) => { await callService(ac, 'warehouse', 'pack', i, 800); });
  r.activity('dispatchCarrier', async (i, ac) => {
    if (i.fail) throw new NonRetryableError('carrier API: no capacity on this route');
    await callService(ac, 'carrier', 'dispatch', i, 900);
    return { trackingId: i.trackingId };
  });
  r.activity('cancelShipment', async (i, ac) => { await callService(ac, 'carrier', 'cancel', i, 600); });
  r.activity('sendConfirmationEmail', async (i, ac) => { await callService(ac, 'email', 'send', i, 500); });
  r.activity('tick', async (i, ac) => { await callService(ac, 'metrics', 'tick', i, 120); });
  r.activity('flakyApi', async (i, ac) => {
    await new Promise((res) => setTimeout(res, 200));
    if (ac.attempt <= i.failures) throw new Error(`503 upstream unavailable (attempt ${ac.attempt})`);
    return `succeeded on attempt ${ac.attempt}`;
  });
  return r;
}

/** Sample launchers shown in the UI. */
export const SAMPLES = [
  { id: 'order', label: 'Order saga', type: 'orderSaga', blurb: 'reserve → charge → 4s review → child fulfilment → email',
    input: () => ({ orderId: 'ORD-' + Math.random().toString(36).slice(2, 7).toUpperCase(), amount: 129, items: ['keyboard', 'cable'] }) },
  { id: 'order-fail', label: 'Payment fails → saga rolls back', type: 'orderSaga', blurb: 'card declined, inventory released in reverse order',
    input: () => ({ orderId: 'ORD-' + Math.random().toString(36).slice(2, 7).toUpperCase(), amount: 89, items: ['monitor'], failAt: 'charge' }) },
  { id: 'order-ship-fail', label: 'Shipping fails → refund + release', type: 'orderSaga', blurb: 'child workflow fails after payment; refund, then release',
    input: () => ({ orderId: 'ORD-' + Math.random().toString(36).slice(2, 7).toUpperCase(), amount: 249, items: ['chair'], failAt: 'ship', fraudReviewMs: 1500 }) },
  { id: 'order-approval', label: 'Order with manager approval', type: 'orderSaga', blurb: 'parks on a signal until a human approves',
    input: () => ({ orderId: 'ORD-' + Math.random().toString(36).slice(2, 7).toUpperCase(), amount: 999, items: ['laptop'], approval: true }) },
  { id: 'retry', label: 'Flaky API with retries', type: 'retryDemo', blurb: 'fails 3x with exponential backoff, then succeeds',
    input: () => ({ failures: 3 }) },
  { id: 'loop', label: 'Long loop (continue-as-new)', type: 'tickLoop', blurb: '24 ticks, history reset every 6',
    input: () => ({ done: 0, total: 24, perRun: 6 }) },
] as const;
