-- gcp-aidp test estate: run in the BigQuery console of the project to migrate.
--
-- Creates the dataset `migration_test` (US multi-region, next to
-- bigquery-public-data.thelook_ecommerce) and fills it with every BigQuery
-- object kind the 0.1 migrator handles. Uses DDL and CREATE TABLE ... AS
-- only (no INSERT), so it also runs in the BigQuery sandbox.
--
-- Cost: the CTAS statements scan a few hundred MB of the public dataset at
-- most, well inside the free tier (1 TB of queries a month). Storage stays
-- far below 10 GB. Remove everything with teardown.sql.

CREATE SCHEMA IF NOT EXISTS migration_test
OPTIONS (location = 'US', description = 'gcp-aidp test estate');

-- 1. Plain tables (explicit columns: thelook's users has a GEOGRAPHY column).
CREATE OR REPLACE TABLE migration_test.users
OPTIONS (description = 'Copied from thelook_ecommerce.users') AS
SELECT id, first_name, last_name, email, age, gender, state, city, country, traffic_source, created_at
FROM `bigquery-public-data.thelook_ecommerce.users`;

CREATE OR REPLACE TABLE migration_test.products AS
SELECT id, name, category, brand, department, sku, cost, retail_price
FROM `bigquery-public-data.thelook_ecommerce.products`;

-- 2a. Partitioned AND clustered: becomes liquid CLUSTER BY (rule D02).
CREATE OR REPLACE TABLE migration_test.orders
PARTITION BY order_date
CLUSTER BY user_id, status AS
SELECT order_id, user_id, status, num_of_item, created_at, DATE(created_at) AS order_date
FROM `bigquery-public-data.thelook_ecommerce.orders`;

-- 2b. DAY partitioning on a DATE column only: an exact PARTITIONED BY (rule D01).
CREATE OR REPLACE TABLE migration_test.order_items
PARTITION BY created_date AS
SELECT id, order_id, product_id, sale_price, DATE(created_at) AS created_date
FROM `bigquery-public-data.thelook_ecommerce.order_items`;

-- 3. STRUCT and ARRAY columns, including an ARRAY of STRUCT.
-- A JOIN + ARRAY_AGG, not a correlated subquery: CTAS refuses those. A stored
-- ARRAY cannot hold a NULL element, hence IFNULL and IGNORE NULLS.
CREATE OR REPLACE TABLE migration_test.customer_profiles AS
SELECT u.id AS customer_id,
       STRUCT(u.city, u.state, u.country) AS address,
       [IFNULL(u.traffic_source, 'unknown'), IFNULL(u.gender, 'unknown')] AS tags,
       ARRAY_AGG(IF(o.order_id IS NULL, NULL, STRUCT(o.order_id, o.status))
                 IGNORE NULLS ORDER BY o.order_id LIMIT 3) AS recent_orders
FROM migration_test.users AS u
LEFT JOIN migration_test.orders AS o ON o.user_id = u.id
WHERE u.id <= 1000
GROUP BY u.id, u.city, u.state, u.country, u.traffic_source, u.gender;

-- 4a. Types the mapper carries, with a caveat or flag: JSON, DATETIME, TIME,
--     parameterized NUMERIC and BIGNUMERIC (P <= 38 fits a Spark DECIMAL).
CREATE OR REPLACE TABLE migration_test.type_carried (
  id INT64 NOT NULL,
  payload JSON,
  local_time DATETIME,
  opening_time TIME,
  amount NUMERIC(10, 2),
  precise_amount BIGNUMERIC(30, 10),
  raw BYTES,
  ratio FLOAT64,
  active BOOL
) AS
SELECT 1, JSON '{"a": 1, "b": [1, 2]}', DATETIME '2026-01-01 10:00:00', TIME '10:30:00.123456',
       NUMERIC '12345678.91', BIGNUMERIC '12345678901234567890.0123456789', b'\x00\x01', 0.25, TRUE
UNION ALL
SELECT 2, JSON '"text"', DATETIME '2026-06-30 23:59:59.999999', TIME '00:00:00',
       NUMERIC '-0.01', BIGNUMERIC '-1', NULL, NULL, FALSE;

-- 4b. Types that block the table by default: GEOGRAPHY, BIGNUMERIC, INTERVAL, RANGE.
CREATE OR REPLACE TABLE migration_test.type_blocked AS
SELECT 1 AS id,
       ST_GEOGPOINT(-122.4, 37.8) AS location,
       BIGNUMERIC '123456789012345678901234567890.123456789' AS big_amount,
       INTERVAL 3 DAY AS dwell,
       RANGE(DATE '2026-01-01', DATE '2026-02-01') AS promo_window;

-- 5. Four views: simple (PASS), rewritable (REVIEW: caveats), QUALIFY (rewritten
--    as a subquery), blocked (a geography function).
CREATE OR REPLACE VIEW migration_test.v_simple AS
SELECT id, email, country
FROM migration_test.users
WHERE country = 'United States';

CREATE OR REPLACE VIEW migration_test.v_rewritable AS
SELECT order_date,
       COUNTIF(status = 'Returned') AS returned_orders,
       SAFE_DIVIDE(SUM(num_of_item), COUNT(*)) AS items_per_order,
       DATE_DIFF(MAX(order_date), MIN(order_date), DAY) AS span_days,
       TIMESTAMP_TRUNC(MAX(created_at), DAY) AS last_order_day
FROM migration_test.orders
GROUP BY order_date;

CREATE OR REPLACE VIEW migration_test.v_latest_order AS
SELECT *
FROM migration_test.orders
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) = 1;

CREATE OR REPLACE VIEW migration_test.v_blocked AS
SELECT order_id, ST_GEOGPOINT(-122.4, 37.8) AS pickup_point
FROM migration_test.orders;

-- 6. A materialized view (becomes a snapshot plus a refresh job, rule M01).
CREATE MATERIALIZED VIEW IF NOT EXISTS migration_test.mv_daily_orders AS
SELECT order_date, COUNT(*) AS orders, SUM(num_of_item) AS items
FROM migration_test.orders
GROUP BY order_date;

-- 7. A SQL function, a JavaScript function and a stored procedure.
CREATE OR REPLACE FUNCTION migration_test.net_price(price FLOAT64, discount FLOAT64)
RETURNS FLOAT64 AS (price * (1 - IFNULL(discount, 0)));

CREATE OR REPLACE FUNCTION migration_test.parse_utm(url STRING)
RETURNS STRING LANGUAGE js AS r"""
  var m = /utm_source=([^&]+)/.exec(url);
  return m ? m[1] : null;
""";

CREATE OR REPLACE PROCEDURE migration_test.refresh_status_summary()
BEGIN
  CREATE OR REPLACE TABLE migration_test.status_summary AS
  SELECT status, COUNT(*) AS n FROM migration_test.orders GROUP BY status;
END;
