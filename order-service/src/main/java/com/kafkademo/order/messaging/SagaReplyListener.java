package com.kafkademo.order.messaging;

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
public class SagaReplyListener {

    private static final Logger log = LoggerFactory.getLogger(SagaReplyListener.class);

    private final EnvelopeCodec codec;
    private final SagaReplyHandler handler;

    public SagaReplyListener(EnvelopeCodec codec, SagaReplyHandler handler) {
        this.codec = codec;
        this.handler = handler;
    }

    @KafkaListener(topics = Topics.SAGA_REPLIES)
    public void onReply(ConsumerRecord<String, String> record) {
        log.info("Reply received: partition={} offset={} key={}", record.partition(), record.offset(), record.key());
        MessageEnvelope envelope = codec.fromJson(record.value());
        handler.handle(envelope);
    }
}
