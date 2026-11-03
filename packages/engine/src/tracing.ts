import { createHash } from 'node:crypto';
import pg from 'pg';
import { context, ROOT_CONTEXT, Span, SpanStatusCode, trace, TraceFlags, Context, Attributes } from '@opentelemetry/api';
import { BasicTracerProvider, ReadableSpan, SimpleSpanProcessor, SpanExporter } from '@opentelemetry/sdk-trace-base';
import { ExportResultCode } from '@opentelemetry/core';

const hrToMs = (t: [number, number]) => t[0] * 1000 + t[1] / 1e6;

/** Writes finished OpenTelemetry spans to Postgres so the UI can draw a per-workflow trace. */
class PgSpanExporter implements SpanExporter {
  constructor(private pool: pg.Pool) {}
  export(spans: ReadableSpan[], done: (r: { code: ExportResultCode; error?: Error }) => void) {
    (async () => {
      for (const s of spans) {
        const wfId = String(s.attributes['workflow.id'] ?? '');
        if (!wfId) continue;
        const sc = s.spanContext();
        await this.pool.query(
          `INSERT INTO spans (workflow_id,trace_id,span_id,parent_span_id,name,start_ms,end_ms,status,attributes)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
          [wfId, sc.traceId, sc.spanId, s.parentSpanId ?? null, s.name, hrToMs(s.startTime), hrToMs(s.endTime),
            s.status.code === SpanStatusCode.ERROR ? 'error' : 'ok', JSON.stringify(s.attributes)]);
      }
    })().then(() => done({ code: ExportResultCode.SUCCESS }), (e) => done({ code: ExportResultCode.FAILED, error: e }));
  }
  async shutdown() { /* pool is owned by the store */ }
}

export const traceIdFor = (rootId: string) => createHash('md5').update(rootId).digest('hex');
export const rootSpanIdFor = (rootId: string) => createHash('sha1').update('root:' + rootId).digest('hex').slice(0, 16);

export interface ActiveSpan {
  ctx: Context;
  span: Span;
  end(opts?: { error?: string; attrs?: Attributes }): void;
}

/**
 * Real OpenTelemetry tracing. The trace id is derived from the root workflow id, so when a second worker
 * takes over after a crash its spans land in the SAME trace as the dead worker's.
 */
export class Tracing {
  private provider: BasicTracerProvider;
  private tracer;
  constructor(pool: pg.Pool) {
    this.provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(new PgSpanExporter(pool))] });
    this.tracer = this.provider.getTracer('durable-flow');
  }
  rootContext(rootId: string): Context {
    return trace.setSpanContext(ROOT_CONTEXT, {
      traceId: traceIdFor(rootId), spanId: rootSpanIdFor(rootId), traceFlags: TraceFlags.SAMPLED, isRemote: true,
    });
  }
  start(name: string, parent: Context, attrs: Attributes): ActiveSpan {
    const span = this.tracer.startSpan(name, { attributes: attrs }, parent);
    const ctx = trace.setSpan(parent, span);
    return {
      ctx, span,
      end: (o) => {
        if (o?.attrs) span.setAttributes(o.attrs);
        if (o?.error) { span.setStatus({ code: SpanStatusCode.ERROR, message: o.error }); span.setAttribute('error.message', o.error); }
        span.end();
      },
    };
  }
  async flush() { await this.provider.forceFlush(); }
}
void context;
