-- Outbox (spec 6.1): a message is first written here, in the same transaction as the business change.
-- OutboxRelay sends it to Kafka afterwards and sets published_at.
CREATE TABLE outbox_event (
    id           UUID         PRIMARY KEY,   -- = the messageId of the Kafka message
    aggregate_id UUID         NOT NULL,      -- the orderId; also the Kafka key
    topic        VARCHAR(100) NOT NULL,
    type         VARCHAR(50)  NOT NULL,
    payload      JSONB        NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL,
    published_at TIMESTAMPTZ                 -- NULL = not sent yet
);

-- Partial index: only unsent rows are in it, so the relay's query stays fast however many sent rows pile up.
CREATE INDEX outbox_event_unpublished ON outbox_event (created_at) WHERE published_at IS NULL;

-- Idempotent consumer (spec 6.2): the messageId of every message already handled.
CREATE TABLE processed_message (
    message_id   UUID        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL
);
