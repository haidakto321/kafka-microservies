package com.kafkademo.order.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

/** Plain unit test: what the relay logs when a send times out (E4b - Kafka down after the producer has metadata). */
@ExtendWith(OutputCaptureExtension.class)
class OutboxRelayWarningTest {

    @Test
    @SuppressWarnings("unchecked")
    void aSendTimeoutIsLoggedWithTheExceptionName(CapturedOutput output) throws Exception {
        OutboxEventRepository outbox = mock(OutboxEventRepository.class);
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        CompletableFuture<SendResult<String, String>> neverConfirmed = mock(CompletableFuture.class);
        // A TimeoutException has no message, so "e.getMessage()" would log "null".
        when(neverConfirmed.get(anyLong(), any(TimeUnit.class))).thenThrow(new TimeoutException());
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(neverConfirmed);
        OutboxEvent event = new OutboxEvent(UUID.randomUUID(), UUID.randomUUID(), Topics.INVENTORY_COMMANDS,
                MessageTypes.RESERVE_STOCK, "{\"productId\":1,\"quantity\":1}", Instant.now());
        when(outbox.lockUnpublished(anyInt())).thenReturn(List.of(event));
        EnvelopeCodec codec = new EnvelopeCodec(new ObjectMapper().findAndRegisterModules());

        new OutboxRelay(outbox, kafka, codec, Clock.systemUTC()).publishPending();

        assertThat(event.getPublishedAt()).isNull();
        assertThat(output).contains("will try again: java.util.concurrent.TimeoutException");
    }
}
