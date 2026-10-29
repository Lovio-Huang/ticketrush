CREATE TABLE users (
    id            BIGINT       NOT NULL AUTO_INCREMENT,
    username      VARCHAR(32)  NOT NULL,
    password_hash VARCHAR(100) NOT NULL,
    role          VARCHAR(16)  NOT NULL,
    created_at    DATETIME(6)  NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_users_username (username)
);

-- "My orders, newest first" without a filesort.
CREATE INDEX idx_orders_user_created ON orders (user_id, created_at);
DROP INDEX idx_orders_user ON orders;

-- Per-event sales breakdown (GROUP BY status) for the admin stats endpoint.
CREATE INDEX idx_orders_event_status ON orders (event_id, status);
