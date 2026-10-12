package com.kafkademo.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/** The real gateway in front of a fake backend that records which paths it receives. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ExtendWith(OutputCaptureExtension.class)
class ApiGatewayTest {

    private static final List<String> RECEIVED = new CopyOnWriteArrayList<>();
    private static final HttpServer BACKEND = fakeBackend();

    @DynamicPropertySource
    static void backends(DynamicPropertyRegistry registry) {
        String url = "http://localhost:" + BACKEND.getAddress().getPort();
        registry.add("services.order-url", () -> url);
        registry.add("services.inventory-url", () -> url);
    }

    @Autowired
    WebTestClient client;

    @BeforeEach
    void clear() {
        RECEIVED.clear();
    }

    @Test
    void ordersAreRoutedWithoutTheApiPrefix() {
        client.get().uri("/api/orders/123").exchange()
                .expectStatus().isOk()
                .expectBody(String.class).isEqualTo("backend");

        assertThat(RECEIVED).containsExactly("GET /orders/123");
    }

    @Test
    void productsAreRoutedWithoutTheApiPrefix() {
        client.get().uri("/api/products/1").exchange().expectStatus().isOk();

        assertThat(RECEIVED).containsExactly("GET /products/1");
    }

    @Test
    void anUnknownPathIs404AndNeverReachesABackend() {
        client.get().uri("/api/nothing").exchange().expectStatus().isNotFound();

        assertThat(RECEIVED).isEmpty();
    }

    @Test
    void everyRequestIsLogged(CapturedOutput output) throws InterruptedException {
        client.post().uri("/api/orders").exchange().expectStatus().isOk();

        // doFinally runs after the response is written, so the line can appear just after the client has it.
        long deadline = System.currentTimeMillis() + 5_000;
        while (!output.getOut().contains("POST /api/orders -> 200 in ") && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertThat(output.getOut()).contains("POST /api/orders -> 200 in ");
    }

    private static HttpServer fakeBackend() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
            server.createContext("/", exchange -> {
                RECEIVED.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
                byte[] body = "backend".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            server.setExecutor(Executors.newCachedThreadPool());
            server.start();
            return server;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
