package com.kafkademo.inventory.outbox;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import java.util.UUID;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OutboxWriter {

    private final OutboxEventRepository outbox;
    private final EnvelopeCodec codec;
    private final OutboxTracing tracing;

    public OutboxWriter(OutboxEventRepository outbox, EnvelopeCodec codec, OutboxTracing tracing) {
        this.outbox = outbox;
        this.codec = codec;
        this.tracing = tracing;
    }

    /**
     * Adds a message to the outbox. MANDATORY: it must run inside the caller's transaction, so the message is
     * saved together with the business change - or not at all. Called without a transaction, it throws.
     * The current trace is stored with it, so the relay can continue that trace when it sends the row.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void add(String topic, UUID orderId, String type, Object payload) {
        MessageEnvelope envelope = codec.wrap(type, orderId, payload);
        outbox.save(new OutboxEvent(envelope.messageId(), orderId, topic, type,
                envelope.payload().toString(), envelope.occurredAt(), tracing.currentTraceParent()));
    }
}
