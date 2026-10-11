package com.kafkademo.inventory.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ReserveStock;
import com.kafkademo.inventory.product.Product;
import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.orm.ObjectOptimisticLockingFailureException;

/** Plain unit test: no Spring, no containers. */
class InventoryCommandListenerRetryTest {

    private final EnvelopeCodec codec = new EnvelopeCodec(new ObjectMapper().findAndRegisterModules());
    private final InventoryCommandHandler handler = mock(InventoryCommandHandler.class);

    @Test
    void aLockConflictIsTriedAgainRightAway() throws Exception {
        doThrow(lockConflict()).doThrow(lockConflict()).doNothing().when(handler).handle(any());

        listener(0).onCommand(aRecord());

        verify(handler, times(3)).handle(any());
    }

    @Test
    void afterThreeLockConflictsTheErrorHandlerTakesOver() {
        doThrow(lockConflict()).when(handler).handle(any());

        assertThatThrownBy(() -> listener(0).onCommand(aRecord()))
                .isInstanceOf(OptimisticLockingFailureException.class);
        verify(handler, times(3)).handle(any());
    }

    @Test
    void theDemoDelayPausesAfterTheCommandIsHandled() throws Exception {
        long start = System.nanoTime();

        listener(300).onCommand(aRecord());

        assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(300));
        verify(handler).handle(any());
    }

    private InventoryCommandListener listener(long delayMs) {
        return new InventoryCommandListener(codec, handler, delayMs);
    }

    private ConsumerRecord<String, String> aRecord() {
        UUID orderId = UUID.randomUUID();
        String json = codec.toJson(codec.wrap(MessageTypes.RESERVE_STOCK, orderId, new ReserveStock(1L, 1)));
        return new ConsumerRecord<>(Topics.INVENTORY_COMMANDS, 0, 0L, orderId.toString(), json);
    }

    private static ObjectOptimisticLockingFailureException lockConflict() {
        return new ObjectOptimisticLockingFailureException(Product.class, 1L);
    }
}
