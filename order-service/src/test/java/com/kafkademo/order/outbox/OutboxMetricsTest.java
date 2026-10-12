package com.kafkademo.order.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

/** Plain unit test: the gauge reads the number of unsent rows each time it is scraped. */
class OutboxMetricsTest {

    @Test
    void theGaugeShowsTheUnsentRows() {
        OutboxEventRepository outbox = mock(OutboxEventRepository.class);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new OutboxMetrics(outbox).bindTo(registry);

        when(outbox.countByPublishedAtIsNull()).thenReturn(3L);
        assertThat(registry.get("outbox.unpublished").gauge().value()).isEqualTo(3.0);

        when(outbox.countByPublishedAtIsNull()).thenReturn(0L);
        assertThat(registry.get("outbox.unpublished").gauge().value()).isZero();
    }
}
