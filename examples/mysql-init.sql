-- Public disposable demo credentials; the Migres source user is read-only.
CREATE USER 'migres'@'%' IDENTIFIED BY 'demo-only-mysql';
GRANT SELECT ON shop.* TO 'migres'@'%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'migres'@'%';

USE shop;
CREATE TABLE orders (
    id BIGINT UNSIGNED NOT NULL,
    customer_id BIGINT UNSIGNED NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    amount DECIMAL(12, 2) NOT NULL,
    PRIMARY KEY (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

INSERT INTO orders (id, customer_id, status, amount) VALUES
    (1, 101, 'pending', 49.90),
    (2, 102, 'paid', 120.00),
    (3, 101, 'pending', 15.50);
