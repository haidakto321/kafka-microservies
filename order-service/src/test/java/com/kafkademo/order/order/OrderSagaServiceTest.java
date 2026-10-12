package com.kafkademo.order.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.order.IntegrationTestContainers;
import com.kafkademo.order.product.ProductClient;
import com.kafkademo.order.product.ProductInfo;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
@Import(IntegrationTestContainers.class)
class OrderSagaServiceTest {

    @Autowired
    OrderSagaService saga;
    @Autowired
    OrderService orderService;
    @Autowired
    OrderRepository orders;
    @Autowired
    JdbcTemplate jdbc;

    @MockitoBean
    ProductClient productClient;

    @BeforeEach
    void stubPrice() {
        when(productClient.getProduct(anyLong())).thenReturn(new ProductInfo(1L, "Keyboard", new BigDecimal("50.00"), 100));
    }

    @Test
    void stockReservedMovesTheOrderToStockReserved() {
        UUID orderId = orderService.placeOrder(1L, 1).getId();

        saga.onStockReserved(orderId);

        assertThat(orders.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.STOCK_RESERVED);
    }

    @Test
    void stockReservationFailedCancelsTheOrder() {
        UUID orderId = orderService.placeOrder(1L, 1).getId();

        saga.onStockReservationFailed(orderId);

        Order order = orders.findById(orderId).orElseThrow();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order.getCancelReason()).isEqualTo(CancelReason.OUT_OF_STOCK);
    }

    @Test
    void aLateStockReservedDoesNotReopenACancelledOrder() {
        UUID orderId = orderService.placeOrder(1L, 1).getId();
        saga.onStockReservationFailed(orderId);

        saga.onStockReserved(orderId);

        assertThat(orders.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    void aLateStockReservationFailedDoesNotCancelAReservedOrder() {
        UUID orderId = orderService.placeOrder(1L, 1).getId();
        saga.onStockReserved(orderId);

        saga.onStockReservationFailed(orderId);

        Order order = orders.findById(orderId).orElseThrow();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.STOCK_RESERVED);
        assertThat(order.getCancelReason()).isNull();
    }

    @Test
    void aDuplicateStockReservedChangesNothing() {
        UUID orderId = orderService.placeOrder(1L, 1).getId();
        saga.onStockReserved(orderId);

        saga.onStockReserved(orderId);

        assertThat(orders.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.STOCK_RESERVED);
    }

    @Test
    void aReplyForAnUnknownOrderIsIgnored() {
        UUID unknown = UUID.randomUUID();

        saga.onStockReserved(unknown);
        saga.onStockReservationFailed(unknown);
        saga.onPaymentSucceeded(unknown);
        saga.onPaymentFailed(unknown);

        assertThat(orders.findById(unknown)).isEmpty();
    }

    @Test
    void stockReservedSendsChargePaymentForTheOrderTotal() {
        UUID orderId = orderService.placeOrder(1L, 1).getId();

        saga.onStockReserved(orderId);

        assertThat(outboxTypes(orderId)).containsExactlyInAnyOrder(MessageTypes.RESERVE_STOCK, MessageTypes.CHARGE_PAYMENT);
        assertThat(jdbc.queryForObject("SELECT (payload->>'amount')::numeric FROM outbox_event WHERE aggregate_id = ? AND type = ?",
                BigDecimal.class, orderId, MessageTypes.CHARGE_PAYMENT)).isEqualByComparingTo("50.00");
    }

    @Test
    void aDuplicateStockReservedChargesOnlyOnce() {
        UUID orderId = orderService.placeOrder(1L, 1).getId();

        saga.onStockReserved(orderId);
        saga.onStockReserved(orderId);

        assertThat(outboxTypes(orderId)).containsExactlyInAnyOrder(MessageTypes.RESERVE_STOCK, MessageTypes.CHARGE_PAYMENT);
    }

    @Test
    void paymentSucceededConfirmsTheOrderAndAnnouncesIt() {
        UUID orderId = orderService.placeOrder(1L, 1).getId();
        saga.onStockReserved(orderId);

        saga.onPaymentSucceeded(orderId);

        assertThat(orders.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(outboxTypes(orderId)).containsExactlyInAnyOrder(
                MessageTypes.RESERVE_STOCK, MessageTypes.CHARGE_PAYMENT, MessageTypes.ORDER_CONFIRMED);
    }

    @Test
    void paymentFailedCancelsTheOrderReleasesTheStockAndAnnouncesIt() {
        UUID orderId = orderService.placeOrder(1L, 1).getId();
        saga.onStockReserved(orderId);

        saga.onPaymentFailed(orderId);

        Order order = orders.findById(orderId).orElseThrow();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order.getCancelReason()).isEqualTo(CancelReason.PAYMENT_FAILED);
        assertThat(outboxTypes(orderId)).containsExactlyInAnyOrder(MessageTypes.RESERVE_STOCK, MessageTypes.CHARGE_PAYMENT,
                MessageTypes.RELEASE_STOCK, MessageTypes.ORDER_CANCELLED);
        assertThat(jdbc.queryForObject("SELECT topic FROM outbox_event WHERE aggregate_id = ? AND type = ?",
                String.class, orderId, MessageTypes.RELEASE_STOCK)).isEqualTo(Topics.INVENTORY_COMMANDS);
    }

    @Test
    void stockReservationFailedAnnouncesTheCancellation() {
        UUID orderId = orderService.placeOrder(1L, 1).getId();

        saga.onStockReservationFailed(orderId);

        assertThat(outboxTypes(orderId)).containsExactlyInAnyOrder(MessageTypes.RESERVE_STOCK, MessageTypes.ORDER_CANCELLED);
        assertThat(jdbc.queryForObject("SELECT payload->>'reason' FROM outbox_event WHERE aggregate_id = ? AND type = ?",
                String.class, orderId, MessageTypes.ORDER_CANCELLED)).isEqualTo("OUT_OF_STOCK");
    }

    @Test
    void aLatePaymentSucceededDoesNotConfirmACancelledOrder() {
        UUID orderId = orderService.placeOrder(1L, 1).getId();
        saga.onStockReservationFailed(orderId);

        saga.onPaymentSucceeded(orderId);

        assertThat(orders.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(outboxTypes(orderId)).doesNotContain(MessageTypes.ORDER_CONFIRMED);
    }

    @Test
    void aPaymentFailedAfterConfirmationChangesNothing() {
        UUID orderId = orderService.placeOrder(1L, 1).getId();
        saga.onStockReserved(orderId);
        saga.onPaymentSucceeded(orderId);

        saga.onPaymentFailed(orderId);

        assertThat(orders.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(outboxTypes(orderId)).doesNotContain(MessageTypes.RELEASE_STOCK, MessageTypes.ORDER_CANCELLED);
    }

    private List<String> outboxTypes(UUID orderId) {
        return jdbc.queryForList("SELECT type FROM outbox_event WHERE aggregate_id = ?", String.class, orderId);
    }
}
