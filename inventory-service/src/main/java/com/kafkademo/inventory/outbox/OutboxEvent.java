package com.kafkademo.inventory.outbox;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/** A message waiting to be sent to Kafka - or already sent, when publishedAt is set. */
@Entity
@Table(name = "outbox_event")
public class OutboxEvent {

    /** The messageId of the Kafka message. It stays the same if the row is sent twice, so consumers spot the resend. */
    @Id
    private UUID id;

    /** The orderId: the Kafka key, and the order this message belongs to. */
    private UUID aggregateId;

    private String topic;

    private String type;

    /** The payload as JSON text, stored in a jsonb column. */
    @JdbcTypeCode(SqlTypes.JSON)
    private String payload;

    private Instant createdAt;

    /** null = not sent yet. */
    private Instant publishedAt;

    protected OutboxEvent() {
        // for JPA
    }

    public OutboxEvent(UUID id, UUID aggregateId, String topic, String type, String payload, Instant createdAt) {
        this.id = id;
        this.aggregateId = aggregateId;
        this.topic = topic;
        this.type = type;
        this.payload = payload;
        this.createdAt = createdAt;
    }

    public void markPublished(Instant now) {
        publishedAt = now;
    }

    public UUID getId() {
        return id;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public String getTopic() {
        return topic;
    }

    public String getType() {
        return type;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }
}
