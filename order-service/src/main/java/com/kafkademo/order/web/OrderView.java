package com.kafkademo.order.web;

import com.kafkademo.order.order.CancelReason;
import com.kafkademo.order.order.Order;
import com.kafkademo.order.order.OrderStatus;
import java.math.BigDecimal;
import java.util.UUID;

/** What the order endpoints return. */
public record OrderView(UUID id, long productId, int quantity, BigDecimal unitPrice, BigDecimal total,
                        OrderStatus status, CancelReason cancelReason) {

    static OrderView from(Order order) {
        return new OrderView(order.getId(), order.getProductId(), order.getQuantity(), order.getUnitPrice(),
                order.getTotal(), order.getStatus(), order.getCancelReason());
    }
}
