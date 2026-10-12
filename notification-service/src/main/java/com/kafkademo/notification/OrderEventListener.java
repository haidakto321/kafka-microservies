package com.kafkademo.notification;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.OrderCancelled;
import com.kafkademo.contracts.payload.OrderConfirmed;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Fan-out: this service reads order-events in its own consumer group, so it gets every event, whoever else reads
 * the topic. NOT idempotent on purpose (spec 6.2): a duplicate event means a duplicate log line - harmless.
 * A real service would send an e-mail; then a duplicate would matter, and it would need a processed-message table.
 */
@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final EnvelopeCodec codec;

    public OrderEventListener(EnvelopeCodec codec) {
        this.codec = codec;
    }

    @KafkaListener(topics = Topics.ORDER_EVENTS)
    public void onEvent(ConsumerRecord<String, String> record) {
        MessageEnvelope envelope = codec.fromJson(record.value());
        switch (envelope.type()) {
            case MessageTypes.ORDER_CONFIRMED -> {
                OrderConfirmed event = codec.payload(envelope, OrderConfirmed.class);
                log.info("NOTIFICATION: order {} confirmed - {} x product {}, total {}",
                        envelope.orderId(), event.quantity(), event.productId(), event.total());
            }
            case MessageTypes.ORDER_CANCELLED -> {
                OrderCancelled event = codec.payload(envelope, OrderCancelled.class);
                log.info("NOTIFICATION: order {} cancelled ({})", envelope.orderId(), event.reason());
            }
            default -> log.warn("Ignoring unknown event type '{}' (messageId={})", envelope.type(), envelope.messageId());
        }
    }
}
