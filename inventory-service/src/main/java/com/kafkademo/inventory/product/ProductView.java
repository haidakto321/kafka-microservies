package com.kafkademo.inventory.product;

import java.math.BigDecimal;

/** What GET /products/{id} returns. */
public record ProductView(long id, String name, BigDecimal price, int stock) {

    static ProductView from(Product product) {
        return new ProductView(product.getId(), product.getName(), product.getPrice(), product.getStock());
    }
}
