CREATE TABLE product (
    id      BIGINT        PRIMARY KEY,
    name    VARCHAR(100)  NOT NULL,
    price   NUMERIC(12,2) NOT NULL,
    stock   INT           NOT NULL CHECK (stock >= 0),
    version BIGINT        NOT NULL DEFAULT 0
);

CREATE TABLE stock_reservation (
    order_id   UUID        PRIMARY KEY,
    product_id BIGINT      NOT NULL REFERENCES product (id),
    quantity   INT         NOT NULL,
    status     VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
