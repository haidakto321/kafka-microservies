package com.kafkademo.contracts;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

/** Turns payloads into the JSON envelope that travels on Kafka, and back. */
public class EnvelopeCodec {

    private final ObjectMapper mapper;
    private final Clock clock;

    public EnvelopeCodec(ObjectMapper mapper) {
        this(mapper, Clock.systemUTC());
    }

    public EnvelopeCodec(ObjectMapper mapper, Clock clock) {
        // Money must keep its exact value and scale. By default Jackson reads a decimal inside a JsonNode payload
        // as a double (100.00 -> 100.0) and can strip zeros in trees (100.00 -> 1E+2). A copy, so the application's
        // shared ObjectMapper is not changed.
        this.mapper = mapper.copy()
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false);
        this.clock = clock;
    }

    /** Wraps a payload in a new envelope with a fresh messageId. */
    public MessageEnvelope wrap(String type, UUID orderId, Object payload) {
        JsonNode body = mapper.valueToTree(payload);
        return new MessageEnvelope(UUID.randomUUID(), type, orderId, Instant.now(clock), body);
    }

    public String toJson(MessageEnvelope envelope) {
        try {
            return mapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not write envelope " + envelope.messageId(), e);
        }
    }

    /**
     * Reads an envelope.
     *
     * @throws InvalidMessageException if the value is null, is not JSON, or misses messageId, type or orderId
     */
    public MessageEnvelope fromJson(String json) {
        if (json == null) {
            // A record with a null value (a "tombstone") has nothing to read.
            throw new InvalidMessageException("Empty message (null value)");
        }
        MessageEnvelope envelope;
        try {
            envelope = mapper.readValue(json, MessageEnvelope.class);
        } catch (JsonProcessingException e) {
            throw new InvalidMessageException("Not a valid message envelope: " + json, e);
        }
        // The JSON text "null" parses to null.
        if (envelope == null || envelope.messageId() == null || envelope.type() == null || envelope.orderId() == null) {
            throw new InvalidMessageException("Envelope is missing messageId, type or orderId: " + json);
        }
        return envelope;
    }

    /**
     * Rebuilds an envelope from stored parts. The outbox relay uses it to send a row exactly as it was written -
     * with the same messageId every time it is sent, so consumers can spot a resend.
     */
    public MessageEnvelope envelope(UUID messageId, String type, UUID orderId, Instant occurredAt, String payloadJson) {
        try {
            return new MessageEnvelope(messageId, type, orderId, occurredAt, mapper.readTree(payloadJson));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Stored payload of message " + messageId + " is not JSON", e);
        }
    }

    /**
     * Reads the payload as the given record type.
     *
     * @throws InvalidMessageException if there is no payload or it does not match the type
     */
    public <T> T payload(MessageEnvelope envelope, Class<T> type) {
        if (envelope.payload() == null || envelope.payload().isNull()) {
            throw new InvalidMessageException("Message " + envelope.messageId() + " has no payload");
        }
        try {
            return mapper.treeToValue(envelope.payload(), type);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new InvalidMessageException(
                    "Payload does not match " + type.getSimpleName() + ": " + envelope.payload(), e);
        }
    }
}
