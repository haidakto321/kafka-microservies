package com.kafkademo.order.messaging;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ReserveStock;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Sends saga commands. The key is always the orderId, so every message of one order lands in one partition. */
@Component
public class CommandPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final EnvelopeCodec codec;

    public CommandPublisher(KafkaTemplate<String, String> kafka, EnvelopeCodec codec) {
        this.kafka = kafka;
        this.codec = codec;
    }

    public void reserveStock(UUID orderId, long productId, int quantity) {
        send(Topics.INVENTORY_COMMANDS, orderId, MessageTypes.RESERVE_STOCK, new ReserveStock(productId, quantity));
    }

    private void send(String topic, UUID orderId, String type, Object payload) {
        MessageEnvelope envelope = codec.wrap(type, orderId, payload);
        try {
            kafka.send(topic, orderId.toString(), codec.toJson(envelope)).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while sending " + type + " for order " + orderId, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Could not send " + type + " for order " + orderId, e);
        }
    }
}
