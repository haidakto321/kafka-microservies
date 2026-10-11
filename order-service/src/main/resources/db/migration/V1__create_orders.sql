CREATE TABLE orders (
    id            UUID          PRIMARY KEY,
    product_id    BIGINT        NOT NULL,
    quantity      INT           NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(12,2) NOT NULL,
    total         NUMERIC(12,2) NOT NULL,
    status        VARCHAR(30)   NOT NULL,
    cancel_reason VARCHAR(30),
    created_at    TIMESTAMPTZ   NOT NULL,
    updated_at    TIMESTAMPTZ   NOT NULL,
    version       BIGINT        NOT NULL DEFAULT 0
);
