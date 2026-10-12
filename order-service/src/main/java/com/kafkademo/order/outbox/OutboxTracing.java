package com.kafkademo.order.outbox;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The outbox breaks the trace on its own: the row is sent later, by another thread (spec 7.3). This class keeps it:
 * when a row is written it stores the current W3C "traceparent"; when the relay sends the row it starts a span
 * from the stored value, and the Kafka producer (observation on) continues the trace in the record headers.
 */
@Component
public class OutboxTracing {

    private static final String TRACEPARENT = "traceparent";

    private final Tracer tracer;
    private final Propagator propagator; // null when tracing is not set up (e.g. in most tests)

    @Autowired
    public OutboxTracing(ObjectProvider<Tracer> tracer, ObjectProvider<Propagator> propagator) {
        this(tracer.getIfAvailable(() -> Tracer.NOOP), propagator.getIfAvailable());
    }

    OutboxTracing(Tracer tracer, Propagator propagator) {
        this.tracer = tracer;
        this.propagator = propagator;
    }

    /** For unit tests: no tracing at all. */
    public static OutboxTracing none() {
        return new OutboxTracing(Tracer.NOOP, null);
    }

    /** The current span as a W3C traceparent, or null when nothing is being traced. */
    public String currentTraceParent() {
        TraceContext context = tracer.currentTraceContext().context();
        if (context == null || propagator == null) {
            return null;
        }
        Map<String, String> carrier = new HashMap<>();
        propagator.inject(context, carrier, Map::put);
        return carrier.get(TRACEPARENT);
    }

    /** A span for sending one row: a child of the stored trace if there is one, else a new trace. */
    public Span startPublishSpan(OutboxEvent event) {
        String name = "outbox publish " + event.getType();
        if (event.getTraceParent() != null && propagator != null) {
            return propagator.extract(Map.of(TRACEPARENT, event.getTraceParent()), Map::get).name(name).start();
        }
        return tracer.nextSpan().name(name).start();
    }

    /** Makes the span current, so the Kafka producer's span becomes its child. */
    public Tracer.SpanInScope inScope(Span span) {
        return tracer.withSpan(span);
    }
}
