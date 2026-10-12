package com.kafkademo.inventory.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.stereotype.Component;

/**
 * Custom metric (spec 7.3): how many outbox rows wait to be sent. A gauge is read when Prometheus scrapes
 * (every 5 s), so it is always the current number. It should stay near 0; growing means Kafka is unreachable.
 */
@Component
public class OutboxMetrics implements MeterBinder {

    private final OutboxEventRepository outbox;

    public OutboxMetrics(OutboxEventRepository outbox) {
        this.outbox = outbox;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("outbox.unpublished", outbox, OutboxEventRepository::countByPublishedAtIsNull)
                .description("Outbox rows not sent to Kafka yet")
                .register(registry);
    }
}
