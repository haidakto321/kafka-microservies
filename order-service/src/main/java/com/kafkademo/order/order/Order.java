package com.kafkademo.order.order;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * An order and its saga state. The state rules live here:
 * PENDING -> STOCK_RESERVED -> CONFIRMED, or -> CANCELLED with a reason.
 */
@Entity
@Table(name = "orders")
public class Order {

    @Id
    private UUID id;

    private long productId;

    private int quantity;

    private BigDecimal unitPrice;

    private BigDecimal total;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    @Enumerated(EnumType.STRING)
    private CancelReason cancelReason;

    private Instant createdAt;

    private Instant updatedAt;

    @Version
    private long version;

    protected Order() {
        // for JPA
    }

    public static Order place(UUID id, long productId, int quantity, BigDecimal unitPrice, Instant now) {
        Order order = new Order();
        order.id = id;
        order.productId = productId;
        order.quantity = quantity;
        order.unitPrice = unitPrice;
        order.total = unitPrice.multiply(BigDecimal.valueOf(quantity));
        order.status = OrderStatus.PENDING;
        order.createdAt = now;
        order.updatedAt = now;
        return order;
    }

    /** @return false if the order is not PENDING - the reply is late or a duplicate and is ignored */
    public boolean markStockReserved(Instant now) {
        if (status != OrderStatus.PENDING) {
            return false;
        }
        status = OrderStatus.STOCK_RESERVED;
        updatedAt = now;
        return true;
    }

    /**
     * Inventory could not reserve the stock. Only a PENDING order is waiting for that answer.
     * @return false if the order is not PENDING - the reply is late or a duplicate and is ignored
     */
    public boolean markStockReservationFailed(Instant now) {
        if (status != OrderStatus.PENDING) {
            return false;
        }
        return cancel(CancelReason.OUT_OF_STOCK, now);
    }

    /** @return false if the order is already CONFIRMED or CANCELLED */
    public boolean cancel(CancelReason reason, Instant now) {
        if (status != OrderStatus.PENDING && status != OrderStatus.STOCK_RESERVED) {
            return false;
        }
        status = OrderStatus.CANCELLED;
        cancelReason = reason;
        updatedAt = now;
        return true;
    }

    public UUID getId() {
        return id;
    }

    public long getProductId() {
        return productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

    public BigDecimal getTotal() {
        return total;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public CancelReason getCancelReason() {
        return cancelReason;
    }
}
