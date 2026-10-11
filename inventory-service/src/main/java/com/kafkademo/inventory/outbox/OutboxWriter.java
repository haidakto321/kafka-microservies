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

    public OutboxWriter(OutboxEventRepository outbox, EnvelopeCodec codec) {
        this.outbox = outbox;
        this.codec = codec;
    }

    /**
     * Adds a message to the outbox. MANDATORY: it must run inside the caller's transaction, so the message is
     * saved together with the business change - or not at all. Called without a transaction, it throws.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void add(String topic, UUID orderId, String type, Object payload) {
        MessageEnvelope envelope = codec.wrap(type, orderId, payload);
        outbox.save(new OutboxEvent(envelope.messageId(), orderId, topic, type,
                envelope.payload().toString(), envelope.occurredAt()));
    }
}
