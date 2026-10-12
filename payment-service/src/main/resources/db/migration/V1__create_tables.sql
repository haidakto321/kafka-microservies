-- One payment per order (natural idempotency: a second one fails on the primary key).
CREATE TABLE payment (
    order_id   UUID          PRIMARY KEY,
    amount     NUMERIC(12,2) NOT NULL,
    status     VARCHAR(20)   NOT NULL,
    reason     VARCHAR(50),
    created_at TIMESTAMPTZ   NOT NULL
);

-- Outbox and idempotent consumer: the same tables as order and inventory (spec 6.1, 6.2).
CREATE TABLE outbox_event (
    id           UUID         PRIMARY KEY,
    aggregate_id UUID         NOT NULL,
    topic        VARCHAR(100) NOT NULL,
    type         VARCHAR(50)  NOT NULL,
    payload      JSONB        NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL,
    published_at TIMESTAMPTZ
);

CREATE INDEX outbox_event_unpublished ON outbox_event (created_at) WHERE published_at IS NULL;

CREATE TABLE processed_message (
    message_id   UUID        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL
);
