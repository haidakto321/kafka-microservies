package com.kafkademo.payment.messaging;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Kafka glue only: reads the record and hands it to the transactional handler. */
@Component
public class PaymentCommandListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentCommandListener.class);

    private final EnvelopeCodec codec;
    private final PaymentCommandHandler handler;

    public PaymentCommandListener(EnvelopeCodec codec, PaymentCommandHandler handler) {
        this.codec = codec;
        this.handler = handler;
    }

    @KafkaListener(topics = Topics.PAYMENT_COMMANDS)
    public void onCommand(ConsumerRecord<String, String> record) {
        log.info("Command received: partition={} offset={} key={}", record.partition(), record.offset(), record.key());
        MessageEnvelope envelope = codec.fromJson(record.value());
        handler.handle(envelope);
    }
}
