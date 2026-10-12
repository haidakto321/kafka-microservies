package com.kafkademo.order.order;

import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ChargePayment;
import com.kafkademo.contracts.payload.OrderCancelled;
import com.kafkademo.contracts.payload.OrderConfirmed;
import com.kafkademo.contracts.payload.ReleaseStock;
import com.kafkademo.order.outbox.OutboxWriter;
import java.time.Clock;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The saga "conductor" (orchestration): reacts to each reply and decides the next step.
 * The state change and the message it causes are saved in the same transaction (outbox):
 * <pre>
 *   StockReserved          -> STOCK_RESERVED, send ChargePayment
 *   StockReservationFailed -> CANCELLED (OUT_OF_STOCK), announce OrderCancelled
 *   PaymentSucceeded       -> CONFIRMED, announce OrderConfirmed
 *   PaymentFailed          -> CANCELLED (PAYMENT_FAILED), send ReleaseStock (compensation), announce OrderCancelled
 * </pre>
 * A reply that does not fit the order's state (late or duplicate) is logged and ignored, and sends nothing.
 */
@Service
public class OrderSagaService {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaService.class);

    private final OrderRepository orders;
    private final OutboxWriter outbox;
    private final Clock clock;

    public OrderSagaService(OrderRepository orders, OutboxWriter outbox, Clock clock) {
        this.orders = orders;
        this.outbox = outbox;
        this.clock = clock;
    }

    @Transactional
    public void onStockReserved(UUID orderId) {
        Optional<Order> found = find(orderId, MessageTypes.STOCK_RESERVED);
        if (found.isEmpty()) {
            return;
        }
        Order order = found.get();
        if (!order.markStockReserved(clock.instant())) {
            ignored(order, MessageTypes.STOCK_RESERVED);
            return;
        }
        outbox.add(Topics.PAYMENT_COMMANDS, orderId, MessageTypes.CHARGE_PAYMENT, new ChargePayment(order.getTotal()));
        log.info("Order {} -> STOCK_RESERVED, ChargePayment {} sent", orderId, order.getTotal());
    }

    @Transactional
    public void onStockReservationFailed(UUID orderId) {
        Optional<Order> found = find(orderId, MessageTypes.STOCK_RESERVATION_FAILED);
        if (found.isEmpty()) {
            return;
        }
        Order order = found.get();
        if (!order.markStockReservationFailed(clock.instant())) {
            ignored(order, MessageTypes.STOCK_RESERVATION_FAILED);
            return;
        }
        outbox.add(Topics.ORDER_EVENTS, orderId, MessageTypes.ORDER_CANCELLED,
                new OrderCancelled(CancelReason.OUT_OF_STOCK.name()));
        log.info("Order {} -> CANCELLED (OUT_OF_STOCK)", orderId);
    }

    @Transactional
    public void onPaymentSucceeded(UUID orderId) {
        Optional<Order> found = find(orderId, MessageTypes.PAYMENT_SUCCEEDED);
        if (found.isEmpty()) {
            return;
        }
        Order order = found.get();
        if (!order.confirm(clock.instant())) {
            ignored(order, MessageTypes.PAYMENT_SUCCEEDED);
            return;
        }
        outbox.add(Topics.ORDER_EVENTS, orderId, MessageTypes.ORDER_CONFIRMED,
                new OrderConfirmed(order.getProductId(), order.getQuantity(), order.getTotal()));
        log.info("Order {} -> CONFIRMED", orderId);
    }

    @Transactional
    public void onPaymentFailed(UUID orderId) {
        Optional<Order> found = find(orderId, MessageTypes.PAYMENT_FAILED);
        if (found.isEmpty()) {
            return;
        }
        Order order = found.get();
        if (!order.markPaymentFailed(clock.instant())) {
            ignored(order, MessageTypes.PAYMENT_FAILED);
            return;
        }
        // Compensation: the stock was already taken, so ask inventory to put it back.
        // A saga cannot roll back another service's commit - it sends a new command that undoes it.
        outbox.add(Topics.INVENTORY_COMMANDS, orderId, MessageTypes.RELEASE_STOCK,
                new ReleaseStock(CancelReason.PAYMENT_FAILED.name()));
        outbox.add(Topics.ORDER_EVENTS, orderId, MessageTypes.ORDER_CANCELLED,
                new OrderCancelled(CancelReason.PAYMENT_FAILED.name()));
        log.info("Order {} -> CANCELLED (PAYMENT_FAILED), ReleaseStock sent", orderId);
    }

    private Optional<Order> find(UUID orderId, String replyType) {
        Optional<Order> found = orders.findById(orderId);
        if (found.isEmpty()) {
            log.warn("{} for unknown order {} - ignored", replyType, orderId);
        }
        return found;
    }

    private static void ignored(Order order, String replyType) {
        log.warn("{} ignored: order {} is already {}", replyType, order.getId(), order.getStatus());
    }
}
