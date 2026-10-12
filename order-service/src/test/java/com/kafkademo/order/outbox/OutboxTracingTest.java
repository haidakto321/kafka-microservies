package com.kafkademo.order.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import com.kafkademo.contracts.Topics;
import com.kafkademo.order.IntegrationTestContainers;
import com.kafkademo.order.order.OrderService;
import com.kafkademo.order.product.ProductClient;
import com.kafkademo.order.product.ProductInfo;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/** Spec 7.3 / E9: the outbox must not break the trace. Tracing is on here (it is off in the other tests). */
@SpringBootTest
@AutoConfigureObservability
@Import(IntegrationTestContainers.class)
class OutboxTracingTest {

    @Autowired
    Tracer tracer;
    @Autowired
    OrderService orderService;
    @Autowired
    JdbcTemplate jdbc;
    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @MockitoBean
    ProductClient productClient;

    @BeforeEach
    void stubPrice() {
        when(productClient.getProduct(anyLong())).thenReturn(new ProductInfo(1L, "Keyboard", new BigDecimal("50.00"), 100));
    }

    @Test
    void theOutboxRowCarriesTheTraceAndTheKafkaRecordContinuesIt() {
        Span request = tracer.nextSpan().name("test request").start();
        UUID orderId;
        try (Tracer.SpanInScope scope = tracer.withSpan(request)) {
            orderId = orderService.placeOrder(1L, 1).getId();
        } finally {
            request.end();
        }
        String traceId = request.context().traceId();

        assertThat(jdbc.queryForObject("SELECT trace_parent FROM outbox_event WHERE aggregate_id = ?", String.class, orderId))
                .startsWith("00-" + traceId + "-");
        // Sent later, by the relay's own thread - and still the same trace.
        assertThat(header(awaitRecord(Topics.INVENTORY_COMMANDS, orderId.toString()), "traceparent"))
                .startsWith("00-" + traceId + "-");
    }

    @Test
    void withoutATraceNothingIsStored() {
        UUID orderId = orderService.placeOrder(1L, 1).getId();

        assertThat(jdbc.queryForObject("SELECT trace_parent FROM outbox_event WHERE aggregate_id = ?", String.class, orderId))
                .isNull();
    }

    private ConsumerRecord<String, String> awaitRecord(String topic, String key) {
        try (Consumer<String, String> consumer = consumerFactory.createConsumer("test-" + UUID.randomUUID(), null)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (key.equals(record.key())) {
                        return record;
                    }
                }
            }
        }
        throw new AssertionError("No record on " + topic + " with key " + key);
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
