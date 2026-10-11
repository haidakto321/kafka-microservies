package com.kafkademo.order.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OrderTest {

    private static final Instant NOW = Instant.parse("2026-10-11T10:00:00Z");

    private Order newOrder() {
        return Order.place(UUID.randomUUID(), 1L, 3, new BigDecimal("50.00"), NOW);
    }

    @Test
    void aNewOrderIsPendingWithTheTotalCalculated() {
        Order order = newOrder();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(order.getTotal()).isEqualByComparingTo("150.00");
        assertThat(order.getCancelReason()).isNull();
    }

    @Test
    void stockReservedMovesPendingToStockReserved() {
        Order order = newOrder();

        assertThat(order.markStockReserved(NOW)).isTrue();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.STOCK_RESERVED);
    }

    @Test
    void stockReservedTwiceIsIgnoredTheSecondTime() {
        Order order = newOrder();
        order.markStockReserved(NOW);

        assertThat(order.markStockReserved(NOW)).isFalse();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.STOCK_RESERVED);
    }

    @Test
    void cancelSetsTheReason() {
        Order order = newOrder();

        assertThat(order.cancel(CancelReason.OUT_OF_STOCK, NOW)).isTrue();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order.getCancelReason()).isEqualTo(CancelReason.OUT_OF_STOCK);
    }

    @Test
    void aCancelledOrderCannotBeReservedOrCancelledAgain() {
        Order order = newOrder();
        order.cancel(CancelReason.OUT_OF_STOCK, NOW);

        assertThat(order.markStockReserved(NOW)).isFalse();
        assertThat(order.cancel(CancelReason.PAYMENT_FAILED, NOW)).isFalse();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order.getCancelReason()).isEqualTo(CancelReason.OUT_OF_STOCK);
    }
}
