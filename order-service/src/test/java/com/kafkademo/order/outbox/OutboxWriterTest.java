package com.kafkademo.order.outbox;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ReserveStock;
import com.kafkademo.order.IntegrationTestContainers;
import com.kafkademo.order.product.ProductClient;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.IllegalTransactionStateException;

@SpringBootTest
@Import(IntegrationTestContainers.class)
class OutboxWriterTest {

    @Autowired
    OutboxWriter outbox;

    /** Same mocks as the other order tests, so Spring reuses their context (and containers). */
    @MockitoBean
    ProductClient productClient;

    /** An outbox row without a transaction around it could be saved without its business change. */
    @Test
    void addingAMessageOutsideATransactionFails() {
        assertThatThrownBy(() -> outbox.add(Topics.INVENTORY_COMMANDS, UUID.randomUUID(),
                MessageTypes.RESERVE_STOCK, new ReserveStock(1L, 1)))
                .isInstanceOf(IllegalTransactionStateException.class);
    }
}
