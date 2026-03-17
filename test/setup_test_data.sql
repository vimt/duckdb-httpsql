-- Test data setup for duckdb-mysql-sharding
-- Run: mysql -h 127.0.0.1 -P 3306 -u root -p < test/setup_test_data.sql
--
-- Creates two databases to simulate multi-host sharding scenarios.
-- Each database contains sharded tables with the _XXXXXXXX suffix convention.

-- ============================================================
-- Database 1: sharding_test_db1
-- ============================================================
DROP DATABASE IF EXISTS sharding_test_db1;
CREATE DATABASE sharding_test_db1;
USE sharding_test_db1;

-- orders: 5 shards
CREATE TABLE orders_00000000 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    status VARCHAR(20) NOT NULL,
    amount DOUBLE NOT NULL,
    quantity INT NOT NULL,
    created_at DATETIME NOT NULL
) ENGINE=InnoDB;

CREATE TABLE orders_00000001 LIKE orders_00000000;
CREATE TABLE orders_00000002 LIKE orders_00000000;
CREATE TABLE orders_00000003 LIKE orders_00000000;
CREATE TABLE orders_00000004 LIKE orders_00000000;

INSERT INTO orders_00000000 (user_id, status, amount, quantity, created_at) VALUES
    (1, 'pending',   10.5,  2, '2024-01-01 10:00:00'),
    (2, 'completed', 20.0,  1, '2024-01-02 11:00:00'),
    (3, 'pending',    5.0,  3, '2024-01-03 12:00:00');

INSERT INTO orders_00000001 (user_id, status, amount, quantity, created_at) VALUES
    (4, 'completed', 30.0,  5, '2024-01-04 09:00:00'),
    (5, 'cancelled', 15.0,  1, '2024-01-05 14:00:00'),
    (6, 'completed', 25.0,  2, '2024-01-06 16:00:00');

INSERT INTO orders_00000002 (user_id, status, amount, quantity, created_at) VALUES
    (7, 'pending',    8.0,  4, '2024-01-07 08:00:00'),
    (8, 'pending',   12.5,  2, '2024-01-08 13:00:00');

INSERT INTO orders_00000003 (user_id, status, amount, quantity, created_at) VALUES
    (9,  'completed', 50.0,  10, '2024-01-09 07:00:00'),
    (10, 'cancelled',  7.5,   1, '2024-01-10 15:00:00'),
    (11, 'pending',    3.0,   1, '2024-01-11 17:00:00'),
    (12, 'completed', 100.0, 20, '2024-01-12 20:00:00');

INSERT INTO orders_00000004 (user_id, status, amount, quantity, created_at) VALUES
    (13, 'cancelled', 22.0,  3, '2024-01-13 11:00:00'),
    (14, 'pending',    9.0,  2, '2024-01-14 12:00:00'),
    (15, 'completed', 45.0,  8, '2024-01-15 18:00:00');

-- users: 3 shards
CREATE TABLE users_00000000 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    age INT
) ENGINE=InnoDB;

CREATE TABLE users_00000001 LIKE users_00000000;
CREATE TABLE users_00000002 LIKE users_00000000;

INSERT INTO users_00000000 (name, email, age) VALUES
    ('Alice', 'alice@test.com', 25),
    ('Bob', 'bob@test.com', 30);

INSERT INTO users_00000001 (name, email, age) VALUES
    ('Carol', 'carol@test.com', 28),
    ('Dave', 'dave@test.com', 35),
    ('Eve', 'eve@test.com', NULL);

INSERT INTO users_00000002 (name, email, age) VALUES
    ('Frank', 'frank@test.com', 22);

-- non_sharded: a regular (non-sharded) table, should appear as-is
CREATE TABLE config (
    id INT AUTO_INCREMENT PRIMARY KEY,
    key_name VARCHAR(50),
    value VARCHAR(200)
) ENGINE=InnoDB;

INSERT INTO config (key_name, value) VALUES ('version', '1.0'), ('env', 'test');

-- ============================================================
-- Database 2: sharding_test_db2 (simulates a second host/schema)
-- ============================================================
DROP DATABASE IF EXISTS sharding_test_db2;
CREATE DATABASE sharding_test_db2;
USE sharding_test_db2;

-- payments: 3 shards
CREATE TABLE payments_00000000 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    method VARCHAR(20) NOT NULL,
    paid_amount DECIMAL(10,2) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE payments_00000001 LIKE payments_00000000;
CREATE TABLE payments_00000002 LIKE payments_00000000;

INSERT INTO payments_00000000 (order_id, method, paid_amount) VALUES
    (1, 'credit_card', 10.50),
    (2, 'debit_card', 20.00);

INSERT INTO payments_00000001 (order_id, method, paid_amount) VALUES
    (3, 'credit_card', 5.00),
    (4, 'bank_transfer', 30.00),
    (5, 'credit_card', 15.00);

INSERT INTO payments_00000002 (order_id, method, paid_amount) VALUES
    (6, 'debit_card', 25.00);

SELECT 'Test data setup complete' AS result;
