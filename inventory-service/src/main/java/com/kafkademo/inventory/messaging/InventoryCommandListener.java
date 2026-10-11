package com.kafkademo.inventory.messaging;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka glue only: reads the record and hands it to the transactional handler.
 * Must NOT be @Transactional itself: the lock-conflict retry below needs each attempt to be its own transaction.
 */
@Component
public class InventoryCommandListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryCommandListener.class);
    private static final int LOCK_CONFLICT_ATTEMPTS = 3;

    private final EnvelopeCodec codec;
    private final InventoryCommandHandler handler;
    private final long processingDelayMs;

    public InventoryCommandListener(EnvelopeCodec codec, InventoryCommandHandler handler,
                                    @Value("${demo.processing-delay-ms:0}") long processingDelayMs) {
        this.codec = codec;
        this.handler = handler;
        this.processingDelayMs = processingDelayMs;
    }

    @KafkaListener(topics = Topics.INVENTORY_COMMANDS)
    public void onCommand(ConsumerRecord<String, String> record) throws InterruptedException {
        // partition + offset in the log make the experiments visible.
        log.info("Command received: partition={} offset={} key={}", record.partition(), record.offset(), record.key());
        MessageEnvelope envelope = codec.fromJson(record.value());
        handleWithLockConflictRetry(envelope);
        if (processingDelayMs > 0) {
            // Demo only (experiments E1 and E6). The DB transaction is committed, but the Kafka offset is not yet:
            // a crash during this pause makes Kafka deliver the message again.
            log.info("Demo delay: waiting {} ms before the offset is committed", processingDelayMs);
            Thread.sleep(processingDelayMs);
        }
    }

    /**
     * Two instances can change the same product at the same moment; the loser's commit fails (optimistic lock).
     * That is normal under load, so try again right away a few times. Leaving it to the error handler would use
     * up its 3 slow retries, and a perfectly valid command could end up in the dead-letter topic.
     */
    private void handleWithLockConflictRetry(MessageEnvelope envelope) {
        for (int attempt = 1; ; attempt++) {
            try {
                handler.handle(envelope);
                return;
            } catch (OptimisticLockingFailureException e) {
                if (attempt == LOCK_CONFLICT_ATTEMPTS) {
                    throw e;
                }
                log.info("Order {}: lock conflict on attempt {}, trying again", envelope.orderId(), attempt);
            }
        }
    }
}
