package com.kafkademo.order.product;

/** inventory-service did not answer (down, slow, 5xx, or the circuit breaker is open). */
public class ProductServiceUnavailableException extends RuntimeException {

    public ProductServiceUnavailableException(Throwable cause) {
        super("Product service unavailable, try again", cause);
    }
}
