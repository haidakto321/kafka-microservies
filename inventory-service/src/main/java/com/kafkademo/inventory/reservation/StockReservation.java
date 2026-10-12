package com.kafkademo.inventory.reservation;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.time.Instant;
import java.util.UUID;
import org.springframework.data.domain.Persistable;

/** Stock held for one order. One reservation per order - enforced by the primary key (see isNew). */
@Entity
@Table(name = "stock_reservation")
public class StockReservation implements Persistable<UUID> {

    @Id
    private UUID orderId;

    private long productId;

    private int quantity;

    @Enumerated(EnumType.STRING)
    private ReservationStatus status;

    private Instant createdAt;

    /** true until the row is saved or loaded. */
    @Transient
    private boolean isNew = true;

    protected StockReservation() {
        // for JPA
    }

    public static StockReservation reserved(UUID orderId, long productId, int quantity, Instant now) {
        StockReservation reservation = new StockReservation();
        reservation.orderId = orderId;
        reservation.productId = productId;
        reservation.quantity = quantity;
        reservation.status = ReservationStatus.RESERVED;
        reservation.createdAt = now;
        return reservation;
    }

    @Override
    public UUID getId() {
        return orderId;
    }

    /**
     * Spring Data's save() asks this to choose INSERT or UPDATE. We set the id ourselves (the orderId), so by
     * default Spring would guess "existing row", load it and UPDATE it - a second reservation for the same order
     * would silently overwrite the first. Returning true makes it an INSERT, and the primary key rejects it.
     */
    @Override
    public boolean isNew() {
        return isNew;
    }

    @PostLoad
    @PostPersist
    void markNotNew() {
        isNew = false;
    }

    /** @return false if it is not RESERVED any more (already released) - then nothing changes */
    public boolean release() {
        if (status != ReservationStatus.RESERVED) {
            return false;
        }
        status = ReservationStatus.RELEASED;
        return true;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public long getProductId() {
        return productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public ReservationStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
