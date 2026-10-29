-- Messages the order consumer gave up on (dead-lettered), so they are visible
-- and can be handled on purpose instead of holding tickets forever unnoticed.
CREATE TABLE order_failures (
    id              BIGINT        NOT NULL AUTO_INCREMENT,
    -- NULL when the message could not even be parsed as an order.
    order_id        VARCHAR(36)   NULL,
    event_id        BIGINT        NULL,
    payload         MEDIUMTEXT    NOT NULL,
    error           VARCHAR(1000) NOT NULL,
    -- Where the first dead letter for it sits, e.g. ticket-orders.DLT-0@42.
    dlt_position    VARCHAR(200)  NOT NULL,
    -- The latest one, so a redelivered repeat isn't counted twice.
    last_dlt_position VARCHAR(200) NOT NULL,
    attempts        INT           NOT NULL,
    status          VARCHAR(20)   NOT NULL,
    note            VARCHAR(500)  NULL,
    first_failed_at DATETIME(6)   NOT NULL,
    last_failed_at  DATETIME(6)   NOT NULL,
    resolved_at     DATETIME(6)   NULL,
    version         BIGINT        NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_order_failures_order (order_id),
    UNIQUE KEY uk_order_failures_position (dlt_position),
    KEY idx_order_failures_status (status)
);
