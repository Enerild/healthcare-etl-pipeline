-- ============================================================
-- ETL Pipeline - PostgreSQL Schema
--
-- Star schema for mobile app event logs, plus audit tables
-- for data quality failures and pipeline run observability.
--
-- Target: PostgreSQL 14+
-- ============================================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS "pgcrypto";  -- for gen_random_uuid()

-- Dimension: Users
CREATE TABLE IF NOT EXISTS dim_users (
    user_id     VARCHAR(64)  PRIMARY KEY,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Dimension: Action Types
CREATE TABLE IF NOT EXISTS dim_actions (
    action_type  VARCHAR(128) PRIMARY KEY,
    category     VARCHAR(64),
    description  TEXT,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Seed known action types observed in source data
INSERT INTO dim_actions (action_type, category) VALUES
    ('login',        'auth'),
    ('logout',       'auth'),
    ('view_item',    'engagement'),
    ('add_to_cart',  'commerce'),
    ('purchase',     'commerce')
ON CONFLICT (action_type) DO NOTHING;

-- Fact: User Actions
CREATE TABLE IF NOT EXISTS fact_user_actions (
      action_id   BIGSERIAL    PRIMARY KEY,
      event_id    VARCHAR(64)  NOT NULL UNIQUE,
      user_id     VARCHAR(64)  NOT NULL REFERENCES dim_users (user_id),
      action_type VARCHAR(128) NOT NULL REFERENCES dim_actions (action_type),
      ts          TIMESTAMPTZ  NOT NULL,
      device      VARCHAR(64),
      location    VARCHAR(128),
      raw_payload JSONB,
      run_id      UUID,
      inserted_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_user_id ON fact_user_actions (user_id);
CREATE INDEX IF NOT EXISTS idx_fact_action  ON fact_user_actions (action_type);
CREATE INDEX IF NOT EXISTS idx_fact_ts      ON fact_user_actions (ts);
CREATE INDEX IF NOT EXISTS idx_fact_run_id  ON fact_user_actions (run_id);
CREATE INDEX IF NOT EXISTS idx_fact_location ON fact_user_actions (location);
CREATE INDEX IF NOT EXISTS idx_fact_device   ON fact_user_actions (device);


-- Audit: Data Quality Failures
CREATE TABLE IF NOT EXISTS dq_failures (
    id          BIGSERIAL    PRIMARY KEY,
    event_id    VARCHAR(64),
    reason      TEXT         NOT NULL,
    raw_record  JSONB,
    failed_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    run_id      UUID
);

CREATE INDEX IF NOT EXISTS idx_dq_run_id ON dq_failures (run_id);

-- Audit: Pipeline Runs
  CREATE TABLE IF NOT EXISTS pipeline_runs (
      run_id        UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
      started_at    TIMESTAMPTZ  NOT NULL,
      finished_at   TIMESTAMPTZ,
      rows_ingested INT          NOT NULL DEFAULT 0,
      rows_clean    INT          NOT NULL DEFAULT 0,
      rows_failed   INT          NOT NULL DEFAULT 0,
      status        TEXT         NOT NULL DEFAULT 'running',
      dq_summary    JSONB
  );
