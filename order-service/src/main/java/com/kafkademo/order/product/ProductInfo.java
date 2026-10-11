package com.kafkademo.order.product;

import java.math.BigDecimal;

/** What inventory-service returns for GET /products/{id}. */
public record ProductInfo(long id, String name, BigDecimal price, int stock) {
}
