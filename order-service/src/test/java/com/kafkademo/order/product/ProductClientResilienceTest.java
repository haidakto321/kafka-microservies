package com.kafkademo.order.product;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.kafkademo.order.IntegrationTestContainers;
import com.kafkademo.order.order.OrderRepository;
import com.sun.net.httpserver.HttpServer;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.client.HttpClientErrorException;

/** The real ProductClient (with its Resilience4j aspects) against a fake inventory that fails on demand. */
@SpringBootTest(properties = "inventory.timeout=500ms")
@AutoConfigureMockMvc
@Import(IntegrationTestContainers.class)
class ProductClientResilienceTest {

    private static final AtomicInteger STATUS = new AtomicInteger(200);
    private static final AtomicLong DELAY_MS = new AtomicLong();
    private static final AtomicInteger HITS = new AtomicInteger();
    private static final HttpServer INVENTORY = fakeInventory();

    @DynamicPropertySource
    static void inventoryUrl(DynamicPropertyRegistry registry) {
        registry.add("inventory.base-url", () -> "http://localhost:" + INVENTORY.getAddress().getPort());
    }

    @Autowired
    ProductClient productClient;
    @Autowired
    CircuitBreakerRegistry breakers;
    @Autowired
    MockMvc mvc;
    @Autowired
    OrderRepository orders;

    @BeforeEach
    void reset() {
        STATUS.set(200);
        DELAY_MS.set(0);
        HITS.set(0);
        breakers.circuitBreaker("inventory").reset();
    }

    @Test
    void a5xxIsRetriedThenBecomes503() {
        STATUS.set(500);

        assertThatThrownBy(() -> productClient.getProduct(1L))
                .isInstanceOf(ProductServiceUnavailableException.class)
                .hasMessage("Product service unavailable, try again");
        assertThat(HITS.get()).isEqualTo(3); // 1 call + 2 retries
    }

    @Test
    void aSlowInventoryTimesOut() {
        DELAY_MS.set(1_500); // longer than the 500 ms timeout of this test

        assertThatThrownBy(() -> productClient.getProduct(1L)).isInstanceOf(ProductServiceUnavailableException.class);
        assertThat(HITS.get()).isEqualTo(3);
    }

    @Test
    void a404IsNotRetriedAndStaysProductNotFound() {
        STATUS.set(404);

        assertThatThrownBy(() -> productClient.getProduct(99L)).isInstanceOf(ProductNotFoundException.class);
        assertThat(HITS.get()).isEqualTo(1);
        assertThat(breakers.circuitBreaker("inventory").getMetrics().getNumberOfFailedCalls()).isZero();
    }

    @Test
    void theBreakerOpensAndThenFailsFast() {
        STATUS.set(500);
        for (int i = 0; i < 4; i++) { // 4 requests x 3 attempts: the 10th failed attempt opens the breaker
            assertThatThrownBy(() -> productClient.getProduct(1L)).isInstanceOf(ProductServiceUnavailableException.class);
        }
        assertThat(breakers.circuitBreaker("inventory").getState()).isEqualTo(CircuitBreaker.State.OPEN);
        int hitsWhenOpen = HITS.get();

        assertThatThrownBy(() -> productClient.getProduct(1L)).isInstanceOf(ProductServiceUnavailableException.class);

        assertThat(HITS.get()).isEqualTo(hitsWhenOpen); // open: inventory is not even called
    }

    @Test
    void theBreakerRecoversThroughHalfOpen() {
        CircuitBreaker breaker = breakers.circuitBreaker("inventory");
        breaker.transitionToOpenState();
        breaker.transitionToHalfOpenState(); // what happens by itself 10 s after opening

        for (int i = 0; i < 3; i++) { // 3 permitted test calls, all good
            assertThat(productClient.getProduct(1L).name()).isEqualTo("Keyboard");
        }

        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    /** Only outages become "try again later". A 400 is a bug or a bad request - retrying would never help. */
    @Test
    void aClientErrorFromInventoryIsNotReportedAsAnOutage() {
        STATUS.set(400);

        assertThatThrownBy(() -> productClient.getProduct(1L))
                .isNotInstanceOf(ProductServiceUnavailableException.class)
                .isInstanceOf(HttpClientErrorException.class);
        assertThat(HITS.get()).isEqualTo(1);
    }

    @Test
    void inventoryDownMeansA503AndNoOrder() throws Exception {
        STATUS.set(503);
        long before = orders.count();

        mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content("{\"productId\":1,\"quantity\":1}"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.detail").value("Product service unavailable, try again"));

        assertThat(orders.count()).isEqualTo(before);
    }

    private static HttpServer fakeInventory() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
            server.createContext("/products", exchange -> {
                HITS.incrementAndGet();
                try {
                    Thread.sleep(DELAY_MS.get());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                int code = STATUS.get();
                if (code == 200) {
                    byte[] body = "{\"id\":1,\"name\":\"Keyboard\",\"price\":50.00,\"stock\":100}"
                            .getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, body.length);
                    exchange.getResponseBody().write(body);
                } else {
                    exchange.sendResponseHeaders(code, -1);
                }
                exchange.close();
            });
            // Many threads: the default is ONE, so a slow request would hold back the next attempts (and count
            // them in the next test).
            server.setExecutor(Executors.newCachedThreadPool());
            server.start();
            return server;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
