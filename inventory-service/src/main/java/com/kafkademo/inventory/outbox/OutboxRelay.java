package com.kafkademo.inventory.outbox;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Sends outbox rows to Kafka (spec 6.1).
 * Every 500 ms: lock up to 100 unsent rows, send them oldest first, waiting for the broker to confirm each one,
 * and mark the sent ones. It stops at the first failure (e.g. Kafka is down): the rest wait for the next run,
 * so a newer message never overtakes an older one.
 * Trade-off: the DB transaction stays open while talking to Kafka. Simple, and fine for a demo (see LEARNINGS).
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH_SIZE = 100;

    private final OutboxEventRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final EnvelopeCodec codec;
    private final Clock clock;

    public OutboxRelay(OutboxEventRepository outbox, KafkaTemplate<String, String> kafka, EnvelopeCodec codec,
                       Clock clock) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.codec = codec;
        this.clock = clock;
    }

    @Scheduled(fixedDelay = 500)
    @Transactional
    public void publishPending() {
        List<OutboxEvent> events = outbox.lockUnpublished(BATCH_SIZE);
        int sent = 0;
        for (OutboxEvent event : events) {
            if (!send(event)) {
                break;
            }
            event.markPublished(clock.instant());
            sent++;
        }
        if (sent > 0) {
            log.info("Outbox relay: sent {} of {} waiting message(s)", sent, events.size());
        }
    }

    private boolean send(OutboxEvent event) {
        MessageEnvelope envelope = codec.envelope(event.getId(), event.getType(), event.getAggregateId(),
                event.getCreatedAt(), event.getPayload());
        try {
            // .get(...) waits for the broker to confirm the write (acks=all).
            kafka.send(event.getTopic(), event.getAggregateId().toString(), codec.toJson(envelope))
                    .get(10, TimeUnit.SECONDS);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            // RuntimeException: the producer can also throw directly instead of failing the future.
            log.warn("Outbox relay: could not send {} {} for order {} - will try again: {}",
                    event.getType(), event.getId(), event.getAggregateId(), e.getMessage());
            return false;
        }
    }
}
