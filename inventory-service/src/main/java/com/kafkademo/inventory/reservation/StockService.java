package com.kafkademo.inventory.reservation;

import com.kafkademo.inventory.product.Product;
import com.kafkademo.inventory.product.ProductRepository;
import java.time.Clock;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class StockService {

    private static final Logger log = LoggerFactory.getLogger(StockService.class);

    private final ProductRepository products;
    private final StockReservationRepository reservations;
    private final Clock clock;

    public StockService(ProductRepository products, StockReservationRepository reservations, Clock clock) {
        this.products = products;
        this.reservations = reservations;
        this.clock = clock;
    }

    /**
     * Takes the items out of stock and records the reservation, in one transaction.
     * M1 has no duplicate protection: the same command twice reserves twice (fixed in M2).
     */
    @Transactional
    public ReservationResult reserve(UUID orderId, long productId, int quantity) {
        Optional<Product> found = products.findById(productId);
        if (found.isEmpty()) {
            log.info("Order {}: product {} does not exist", orderId, productId);
            return ReservationResult.failed(ReservationResult.UNKNOWN_PRODUCT);
        }
        Product product = found.get();
        if (!product.hasStock(quantity)) {
            log.info("Order {}: only {} of product {} left, {} wanted", orderId, product.getStock(), productId, quantity);
            return ReservationResult.failed(ReservationResult.OUT_OF_STOCK);
        }
        product.decreaseStock(quantity);
        reservations.save(StockReservation.reserved(orderId, productId, quantity, clock.instant()));
        log.info("Order {}: reserved {} of product {}, {} left", orderId, quantity, productId, product.getStock());
        return ReservationResult.success();
    }

    /**
     * Compensation (the payment failed): puts the reserved items back.
     * Natural idempotency: only a RESERVED reservation is released, so a repeated ReleaseStock changes nothing.
     *
     * @return true if stock was put back
     */
    @Transactional
    public boolean release(UUID orderId) {
        Optional<StockReservation> found = reservations.findById(orderId);
        if (found.isEmpty()) {
            log.info("Order {}: nothing to release (no reservation)", orderId);
            return false;
        }
        StockReservation reservation = found.get();
        if (!reservation.release()) {
            log.info("Order {}: reservation is already {}, nothing to release", orderId, reservation.getStatus());
            return false;
        }
        Product product = products.findById(reservation.getProductId()).orElseThrow();
        product.increaseStock(reservation.getQuantity());
        log.info("Order {}: released {} of product {}, {} left", orderId, reservation.getQuantity(),
                product.getId(), product.getStock());
        return true;
    }
}
