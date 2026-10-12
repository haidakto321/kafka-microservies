package com.kafkademo.order.product;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Synchronous REST call to inventory-service for the product price, protected by Resilience4j (spec 7.2):
 * timeout 2 s (RestClient, see AppConfig), up to 3 attempts, and a circuit breaker.
 * Order of the aspects: Retry( CircuitBreaker( call ) ) - every attempt is counted by the breaker.
 */
@Component
public class ProductClient {

    static final String INVENTORY = "inventory";

    private final RestClient restClient;

    public ProductClient(RestClient.Builder builder, @Value("${inventory.base-url}") String baseUrl) {
        this.restClient = builder.baseUrl(baseUrl).build();
    }

    @Retry(name = INVENTORY, fallbackMethod = "unavailable")
    @CircuitBreaker(name = INVENTORY)
    public ProductInfo getProduct(long productId) {
        return restClient.get()
                .uri("/products/{id}", productId)
                .retrieve()
                .onStatus(status -> status.isSameCodeAs(HttpStatus.NOT_FOUND), (request, response) -> {
                    throw new ProductNotFoundException(productId);
                })
                .body(ProductInfo.class);
    }

    /**
     * Called after the last failed attempt, or at once when the breaker is open. Fail fast with a 503 -
     * never guess a price. A 404 is an answer, not an outage, so it is passed on unchanged.
     */
    private ProductInfo unavailable(long productId, Throwable cause) {
        if (cause instanceof ProductNotFoundException notFound) {
            throw notFound;
        }
        throw new ProductServiceUnavailableException(cause);
    }
}
