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

    /** The second guard behind the processed-message check: one reservation per order, enforced by the primary key. */
    @Test
    void aSecondReservationForTheSameOrderIsRejected() {
        long productId = ThreadLocalRandom.current().nextLong(10_000, 1_000_000_000L);
        products.save(new Product(productId, "Guarded", new BigDecimal("1.00"), 5));
        UUID orderId = UUID.randomUUID();
        stockService.reserve(orderId, productId, 1);

        assertThatThrownBy(() -> stockService.reserve(orderId, productId, 1))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThat(products.findById(productId).orElseThrow().getStock()).isEqualTo(4);
    }
}
