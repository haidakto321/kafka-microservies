package com.kafkademo.order.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import com.kafkademo.order.IntegrationTestContainers;
import com.kafkademo.order.product.ProductClient;
import com.kafkademo.order.product.ProductInfo;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
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

        assertThat(orders.findById(unknown)).isEmpty();
    }
}
