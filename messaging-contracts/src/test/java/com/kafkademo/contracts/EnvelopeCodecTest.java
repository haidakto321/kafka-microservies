package com.kafkademo.contracts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.kafkademo.contracts.payload.ReserveStock;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class EnvelopeCodecTest {

    private static final Instant NOW = Instant.parse("2026-10-11T10:00:00Z");

    private final ObjectMapper mapper = new ObjectMapper()
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private final EnvelopeCodec codec = new EnvelopeCodec(mapper, Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void wrapsAndReadsBackAPayload() {
        UUID orderId = UUID.randomUUID();
        MessageEnvelope sent = codec.wrap(MessageTypes.RESERVE_STOCK, orderId, new ReserveStock(1L, 2));

        MessageEnvelope received = codec.fromJson(codec.toJson(sent));

        assertThat(received.messageId()).isEqualTo(sent.messageId());
        assertThat(received.type()).isEqualTo("ReserveStock");
        assertThat(received.orderId()).isEqualTo(orderId);
        assertThat(received.occurredAt()).isEqualTo(NOW);
        assertThat(codec.payload(received, ReserveStock.class)).isEqualTo(new ReserveStock(1L, 2));
    }

    @Test
    void writesReadableJson() {
        String json = codec.toJson(codec.wrap(MessageTypes.RESERVE_STOCK, UUID.randomUUID(), new ReserveStock(7L, 3)));

        assertThat(json)
                .contains("\"type\":\"ReserveStock\"")
                .contains("\"occurredAt\":\"2026-10-11T10:00:00Z\"")
                .contains("\"payload\":{\"productId\":7,\"quantity\":3}");
    }

    @Test
    void everyEnvelopeGetsItsOwnMessageId() {
        UUID orderId = UUID.randomUUID();

        MessageEnvelope first = codec.wrap(MessageTypes.RESERVE_STOCK, orderId, new ReserveStock(1L, 1));
        MessageEnvelope second = codec.wrap(MessageTypes.RESERVE_STOCK, orderId, new ReserveStock(1L, 1));

        assertThat(first.messageId()).isNotEqualTo(second.messageId());
    }

    @Test
    void rejectsTextThatIsNotJson() {
        assertThatThrownBy(() -> codec.fromJson("not json"))
                .isInstanceOf(InvalidMessageException.class);
    }

    @Test
    void rejectsAnEnvelopeWithoutOrderId() {
        String json = "{\"messageId\":\"" + UUID.randomUUID() + "\",\"type\":\"ReserveStock\",\"payload\":{}}";

        assertThatThrownBy(() -> codec.fromJson(json))
                .isInstanceOf(InvalidMessageException.class)
                .hasMessageContaining("missing");
    }

    @Test
    void rejectsAPayloadOfTheWrongShape() {
        String json = "{\"messageId\":\"" + UUID.randomUUID() + "\",\"type\":\"ReserveStock\",\"orderId\":\""
                + UUID.randomUUID() + "\",\"payload\":{\"productId\":\"abc\"}}";
        MessageEnvelope envelope = codec.fromJson(json);

        assertThatThrownBy(() -> codec.payload(envelope, ReserveStock.class))
                .isInstanceOf(InvalidMessageException.class);
    }

    @Test
    void rejectsAnEnvelopeWithoutPayload() {
        String json = "{\"messageId\":\"" + UUID.randomUUID() + "\",\"type\":\"ReserveStock\",\"orderId\":\""
                + UUID.randomUUID() + "\"}";
        MessageEnvelope envelope = codec.fromJson(json);

        assertThatThrownBy(() -> codec.payload(envelope, ReserveStock.class))
                .isInstanceOf(InvalidMessageException.class)
                .hasMessageContaining("no payload");
    }
}
