package com.kafkademo.contracts;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
        this.mapper = mapper;
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
     * @throws InvalidMessageException if the text is not JSON or misses messageId, type or orderId
     */
    public MessageEnvelope fromJson(String json) {
        MessageEnvelope envelope;
        try {
            envelope = mapper.readValue(json, MessageEnvelope.class);
        } catch (JsonProcessingException e) {
            throw new InvalidMessageException("Not a valid message envelope: " + json, e);
        }
        if (envelope.messageId() == null || envelope.type() == null || envelope.orderId() == null) {
            throw new InvalidMessageException("Envelope is missing messageId, type or orderId: " + json);
        }
        return envelope;
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
