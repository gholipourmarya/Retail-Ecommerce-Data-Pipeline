CREATE SCHEMA raw;
CREATE SCHEMA reporting;

CREATE TABLE raw.orders (
    source_row integer PRIMARY KEY,
    order_id text NOT NULL,
    product_id text NOT NULL,
    quantity numeric NOT NULL CHECK (quantity >= 0),
    unit_price numeric NOT NULL CHECK (unit_price >= 0),
    stock numeric NOT NULL CHECK (stock >= 0)
);

CREATE TABLE raw.rejected_orders (
    source_row integer PRIMARY KEY,
    payload jsonb NOT NULL,
    reason text NOT NULL
);

CREATE TABLE raw.stream_orders (
    topic text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    payload text,
    event_id text UNIQUE,
    event_time timestamptz,
    product_id text,
    revenue numeric,
    PRIMARY KEY (topic, kafka_partition, kafka_offset)
);

CREATE VIEW reporting.stream_product_sales AS
SELECT product_id, count(*) AS events, sum(revenue) AS revenue
FROM raw.stream_orders
WHERE event_id IS NOT NULL
GROUP BY product_id;

CREATE ROLE grafana_reader LOGIN PASSWORD 'grafana_demo';
GRANT USAGE ON SCHEMA reporting TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA reporting TO grafana_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA reporting GRANT SELECT ON TABLES TO grafana_reader;
