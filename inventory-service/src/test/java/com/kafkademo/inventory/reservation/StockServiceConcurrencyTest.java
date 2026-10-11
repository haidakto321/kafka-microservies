package com.kafkademo.inventory.reservation;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkademo.inventory.IntegrationTestContainers;
import com.kafkademo.inventory.product.Product;
import com.kafkademo.inventory.product.ProductRepository;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.orm.ObjectOptimisticLockingFailureException;

@SpringBootTest
@Import(IntegrationTestContainers.class)
class StockServiceConcurrencyTest {

    @Autowired
    StockService stockService;
    @Autowired
    ProductRepository products;
    @Autowired
    StockReservationRepository reservations;

    /** Two consumer instances may handle two orders for the same product at the same moment. */
    @Test
    void twoOrdersForTheLastItemNeverOversell() throws Exception {
        long productId = ThreadLocalRandom.current().nextLong(10_000, 1_000_000_000L);
        products.save(new Product(productId, "Last one", new BigDecimal("9.99"), 1));

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                results.add(pool.submit(() -> {
                    start.await();
                    try {
                        return stockService.reserve(UUID.randomUUID(), productId, 1).reserved();
                    } catch (ObjectOptimisticLockingFailureException e) {
                        // The losing transaction. In the real flow the Kafka error handler retries it,
                        // and the retry then sees no stock left.
                        return false;
                    }
                }));
            }
            start.countDown();

            long successes = 0;
            for (Future<Boolean> result : results) {
                if (result.get(30, TimeUnit.SECONDS)) {
                    successes++;
                }
            }

            assertThat(successes).isEqualTo(1);
            assertThat(products.findById(productId).orElseThrow().getStock()).isZero();
            assertThat(reservations.findAll()).filteredOn(r -> r.getProductId() == productId).hasSize(1);
        } finally {
            pool.shutdownNow();
        }
    }
}
