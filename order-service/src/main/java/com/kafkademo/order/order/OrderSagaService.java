package com.kafkademo.order.order;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The saga "conductor": reacts to replies and decides the next step.
 * M1: stock replies only. M3 adds payment and compensation here.
 */
@Service
public class OrderSagaService {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaService.class);

    private final OrderRepository orders;
    private final Clock clock;

    public OrderSagaService(OrderRepository orders, Clock clock) {
        this.orders = orders;
        this.clock = clock;
    }

    @Transactional
    public void onStockReserved(UUID orderId) {
        Optional<Order> found = orders.findById(orderId);
        if (found.isEmpty()) {
            log.warn("StockReserved for unknown order {} - ignored", orderId);
            return;
        }
        Order order = found.get();
        if (order.markStockReserved(clock.instant())) {
            log.info("Order {} -> STOCK_RESERVED", orderId);
        } else {
            log.warn("StockReserved ignored: order {} is already {}", orderId, order.getStatus());
        }
    }

    @Transactional
    public void onStockReservationFailed(UUID orderId) {
        Optional<Order> found = orders.findById(orderId);
        if (found.isEmpty()) {
            log.warn("StockReservationFailed for unknown order {} - ignored", orderId);
            return;
        }
        Order order = found.get();
        if (order.markStockReservationFailed(clock.instant())) {
            log.info("Order {} -> CANCELLED (OUT_OF_STOCK)", orderId);
        } else {
            log.warn("StockReservationFailed ignored: order {} is already {}", orderId, order.getStatus());
        }
    }
}
