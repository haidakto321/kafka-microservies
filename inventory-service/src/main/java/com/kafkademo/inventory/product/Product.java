package com.kafkademo.inventory.product;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;

@Entity
@Table(name = "product")
public class Product {

    @Id
    private Long id;

    private String name;

    private BigDecimal price;

    private int stock;

    /** Optimistic locking: two transactions changing the same product at once - the second one fails. */
    @Version
    private long version;

    protected Product() {
        // for JPA
    }

    public Product(Long id, String name, BigDecimal price, int stock) {
        this.id = id;
        this.name = name;
        this.price = price;
        this.stock = stock;
    }

    public boolean hasStock(int quantity) {
        return quantity > 0 && stock >= quantity;
    }

    public void decreaseStock(int quantity) {
        if (!hasStock(quantity)) {
            throw new IllegalStateException(
                    "Not enough stock for product " + id + ": have " + stock + ", need " + quantity);
        }
        stock -= quantity;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public int getStock() {
        return stock;
    }
}
