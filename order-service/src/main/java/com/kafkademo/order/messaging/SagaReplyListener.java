package com.kafkademo.order.messaging;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.order.order.OrderSagaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SagaReplyListener {

    private static final Logger log = LoggerFactory.getLogger(SagaReplyListener.class);

    private final EnvelopeCodec codec;
    private final OrderSagaService saga;

    public SagaReplyListener(EnvelopeCodec codec, OrderSagaService saga) {
        this.codec = codec;
        this.saga = saga;
    }

    @KafkaListener(topics = Topics.SAGA_REPLIES)
    public void onReply(ConsumerRecord<String, String> record) {
        log.info("Reply received: partition={} offset={} key={}", record.partition(), record.offset(), record.key());
        MessageEnvelope envelope = codec.fromJson(record.value());
        switch (envelope.type()) {
            case MessageTypes.STOCK_RESERVED -> saga.onStockReserved(envelope.orderId());
            case MessageTypes.STOCK_RESERVATION_FAILED -> saga.onStockReservationFailed(envelope.orderId());
            default -> log.warn("Ignoring unknown reply type '{}' (messageId={})", envelope.type(), envelope.messageId());
        }
    }
}
