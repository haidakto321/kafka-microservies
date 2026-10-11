package com.kafkademo.inventory.messaging;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.Topics;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Sends replies to order-service on saga-replies. The key is the orderId, so replies stay in order per order. */
@Component
public class ReplyPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final EnvelopeCodec codec;

    public ReplyPublisher(KafkaTemplate<String, String> kafka, EnvelopeCodec codec) {
        this.kafka = kafka;
        this.codec = codec;
    }

    public void send(UUID orderId, String type, Object payload) {
        MessageEnvelope envelope = codec.wrap(type, orderId, payload);
        try {
            // .get(...) waits for the broker to confirm the write (acks=all).
            kafka.send(Topics.SAGA_REPLIES, orderId.toString(), codec.toJson(envelope)).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while sending " + type + " for order " + orderId, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Could not send " + type + " for order " + orderId, e);
        }
    }
}
