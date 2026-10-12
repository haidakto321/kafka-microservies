package com.kafkademo.payment.payment;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.springframework.data.domain.Persistable;

/** A (fake) payment. One per order - enforced by the primary key (insert-only, see isNew). */
@Entity
@Table(name = "payment")
public class Payment implements Persistable<UUID> {

    /** The fake rule (spec): anything over 1,000 fails. The Laptop (1,500) always does. */
    public static final BigDecimal LIMIT = new BigDecimal("1000.00");
    public static final String OVER_LIMIT = "AMOUNT_OVER_LIMIT";

    @Id
    private UUID orderId;

    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    private PaymentStatus status;

    private String reason;

    private Instant createdAt;

    /** true until the row is saved or loaded: makes save() an INSERT, never an update. */
    @Transient
    private boolean isNew = true;

    protected Payment() {
        // for JPA
    }

    public static Payment charge(UUID orderId, BigDecimal amount, Instant now) {
        Payment payment = new Payment();
        payment.orderId = orderId;
        payment.amount = amount;
        payment.createdAt = now;
        if (amount.compareTo(LIMIT) <= 0) {
            payment.status = PaymentStatus.SUCCEEDED;
        } else {
            payment.status = PaymentStatus.FAILED;
            payment.reason = OVER_LIMIT;
        }
        return payment;
    }

    public boolean succeeded() {
        return status == PaymentStatus.SUCCEEDED;
    }

    @Override
    public UUID getId() {
        return orderId;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    @PostLoad
    @PostPersist
    void markNotNew() {
        isNew = false;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public PaymentStatus getStatus() {
        return status;
    }

    public String getReason() {
        return reason;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
