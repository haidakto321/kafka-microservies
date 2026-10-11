package com.kafkademo.contracts;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

/**
 * The JSON shape of every Kafka message in this project.
 *
 * @param messageId  unique per message - used later to spot duplicates
 * @param type       which message this is, e.g. "ReserveStock" (see {@link MessageTypes})
 * @param orderId    the order this message belongs to; also the Kafka key
 * @param occurredAt when the message was created
 * @param payload    the message body; read it with {@link EnvelopeCodec#payload}
 */
public record MessageEnvelope(UUID messageId, String type, UUID orderId, Instant occurredAt, JsonNode payload) {
}
