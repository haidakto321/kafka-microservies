package com.kafkademo.inventory.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkademo.inventory.IntegrationTestContainers;
import com.kafkademo.inventory.product.Product;
import com.kafkademo.inventory.product.ProductRepository;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;

@SpringBootTest
@Import(IntegrationTestContainers.class)
class StockServiceTest {

    @Autowired
    StockService stockService;
    @Autowired
    ProductRepository products;
    @Autowired
    StockReservationRepository reservations;

    /** The second guard behind the processed-message check: one reservation per order, enforced by the primary key. */
    @Test
    void aSecondReservationForTheSameOrderIsRejected() {
        long productId = newProduct(5);
        UUID orderId = UUID.randomUUID();
        stockService.reserve(orderId, productId, 1);

        assertThatThrownBy(() -> stockService.reserve(orderId, productId, 1))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThat(products.findById(productId).orElseThrow().getStock()).isEqualTo(4);
    }

    @Test
    void releasePutsTheReservedItemsBack() {
        long productId = newProduct(5);
        UUID orderId = UUID.randomUUID();
        stockService.reserve(orderId, productId, 2);

        assertThat(stockService.release(orderId)).isTrue();

        assertThat(products.findById(productId).orElseThrow().getStock()).isEqualTo(5);
        assertThat(reservations.findById(orderId).orElseThrow().getStatus()).isEqualTo(ReservationStatus.RELEASED);
    }

    /** Natural idempotency: only a RESERVED reservation is released, so a repeated ReleaseStock is harmless. */
    @Test
    void aSecondReleaseChangesNothing() {
        long productId = newProduct(5);
        UUID orderId = UUID.randomUUID();
        stockService.reserve(orderId, productId, 2);
        stockService.release(orderId);

        assertThat(stockService.release(orderId)).isFalse();

        assertThat(products.findById(productId).orElseThrow().getStock()).isEqualTo(5);
    }

    @Test
    void releasingWithoutAReservationChangesNothing() {
        assertThat(stockService.release(UUID.randomUUID())).isFalse();
    }

    private long newProduct(int stock) {
        long id = ThreadLocalRandom.current().nextLong(10_000, 1_000_000_000L);
        products.save(new Product(id, "Test product " + id, new BigDecimal("1.00"), stock));
        return id;
    }
}
