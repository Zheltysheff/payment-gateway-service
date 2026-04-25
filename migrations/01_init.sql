CREATE TYPE payment_status AS ENUM (
    'NEW',
    'PROCESSING',
    'COMPLETED',
    'FAILED',
    'CANCELED'
);

CREATE TABLE IF NOT EXISTS payments (
    id          UUID PRIMARY KEY,
    amount      BIGINT         NOT NULL CHECK (amount > 0),
    currency    CHAR(3)        NOT NULL,
    merchant_id TEXT           NOT NULL,
    order_id    TEXT           NOT NULL,
    user_id     TEXT           NOT NULL,
    status      payment_status NOT NULL,
    created_at  TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    UNIQUE (merchant_id, order_id)
);

CREATE TABLE payment_events (
    id          BIGSERIAL   PRIMARY KEY,
    payment_id  UUID        NOT NULL,
    event_type  TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE processed_commands (
    payment_id   UUID        PRIMARY KEY,
    command_type TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
