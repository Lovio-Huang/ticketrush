CREATE TABLE events (
    id                BIGINT       NOT NULL AUTO_INCREMENT,
    name              VARCHAR(200) NOT NULL,
    venue             VARCHAR(200) NOT NULL,
    starts_at         DATETIME(6)  NOT NULL,
    sale_starts_at    DATETIME(6)  NOT NULL,
    total_tickets     INT          NOT NULL,
    available_tickets INT          NOT NULL,
    price_cents       INT          NOT NULL,
    per_user_limit    INT          NOT NULL,
    created_at        DATETIME(6)  NOT NULL,
    PRIMARY KEY (id),
    -- Last line of defence against overselling: MySQL rejects any update that
    -- would push inventory below zero, even if the application has a bug.
    CONSTRAINT chk_events_available CHECK (available_tickets >= 0 AND available_tickets <= total_tickets)
);

CREATE TABLE orders (
    id              VARCHAR(36) NOT NULL,
    event_id        BIGINT      NOT NULL,
    user_id         VARCHAR(64) NOT NULL,
    quantity        INT         NOT NULL,
    amount_cents    INT         NOT NULL,
    status          VARCHAR(20) NOT NULL,
    idempotency_key VARCHAR(64) NOT NULL,
    expires_at      DATETIME(6) NOT NULL,
    created_at      DATETIME(6) NOT NULL,
    updated_at      DATETIME(6) NOT NULL,
    version         BIGINT      NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_orders_event_user_idem (event_id, user_id, idempotency_key),
    KEY idx_orders_status_expires (status, expires_at),
    KEY idx_orders_user (user_id),
    CONSTRAINT fk_orders_event FOREIGN KEY (event_id) REFERENCES events (id)
);
