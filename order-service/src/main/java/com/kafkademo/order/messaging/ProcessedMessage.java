package com.kafkademo.order.messaging;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.time.Instant;
import java.util.UUID;
import org.springframework.data.domain.Persistable;

/** "This message was already handled." Saved in the same transaction as the work the message caused. */
@Entity
@Table(name = "processed_message")
public class ProcessedMessage implements Persistable<UUID> {

    @Id
    private UUID messageId;

    private Instant processedAt;

    /**
     * true until the row is saved or loaded. Spring Data's save() asks isNew() to choose INSERT or UPDATE; the id
     * is set by us, so by default it would guess UPDATE. INSERT means two copies racing each other cannot both win.
     */
    @Transient
    private boolean isNew = true;

    protected ProcessedMessage() {
        // for JPA
    }

    public ProcessedMessage(UUID messageId, Instant processedAt) {
        this.messageId = messageId;
        this.processedAt = processedAt;
    }

    @Override
    public UUID getId() {
        return messageId;
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

    public Instant getProcessedAt() {
        return processedAt;
    }
}
