BEGIN;

CREATE TABLE IF NOT EXISTS raw.csv_checkpoint (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    byte_offset bigint NOT NULL,
    source_row integer NOT NULL,
    prefix_sha256 text NOT NULL
);

CREATE INDEX IF NOT EXISTS orders_product_id_idx ON raw.orders (product_id);

ALTER TABLE IF EXISTS reporting.product_sales
    ADD COLUMN IF NOT EXISTS last_source_row integer;

CREATE TABLE IF NOT EXISTS raw.pipeline_runs (
    run_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline text NOT NULL CHECK (pipeline IN ('csv', 'kafka')),
    batch_id bigint,
    finished_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    rows_seen bigint NOT NULL,
    rows_rejected bigint NOT NULL,
    rows_written bigint NOT NULL,
    duration_seconds double precision NOT NULL,
    alert_threshold double precision NOT NULL
);

CREATE OR REPLACE VIEW reporting.pipeline_quality AS
SELECT
    run_id, pipeline, batch_id, finished_at, rows_seen,
    rows_seen - rows_rejected AS rows_valid, rows_rejected, rows_written,
    rows_seen - rows_written AS rows_skipped,
    coalesce(rows_rejected::double precision / nullif(rows_seen, 0), 0) AS error_rate,
    duration_seconds, alert_threshold,
    coalesce(rows_rejected::double precision / nullif(rows_seen, 0), 0) > alert_threshold AS alert
FROM raw.pipeline_runs;

GRANT SELECT ON reporting.pipeline_quality TO grafana_reader;

COMMIT;
