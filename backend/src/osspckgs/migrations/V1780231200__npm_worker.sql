-- npm worker supporting tables and partition management for download tracking.

ALTER TABLE maintainers DROP COLUMN IF EXISTS email_hash;
ALTER TABLE maintainers ADD COLUMN IF NOT EXISTS email text;

CREATE TABLE npm_worker_state (
  name        text PRIMARY KEY,
  value       text NOT NULL,
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE npm_package_state (
  name             text        PRIMARY KEY,
  first_scanned_at timestamptz NOT NULL DEFAULT now()
);

-- ============================================================
-- pg_partman setup for downloads_daily (monthly partitions)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS partman;
CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;

SELECT partman.create_parent(
  p_parent_table => 'public.downloads_daily',
  p_control      => 'date',
  p_interval     => '1 month',
  p_premake      => 12
);

-- Create all historical monthly partitions (2015-01 through last month).
DO $$
DECLARE
  m date;
BEGIN
  FOR m IN
    SELECT d::date
      FROM generate_series(
        '2015-01-01'::date,
        (date_trunc('month', now()) - interval '1 month')::date,
        '1 month'::interval
      ) AS d
  LOOP
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS %I PARTITION OF downloads_daily FOR VALUES FROM (%L) TO (%L)',
      'downloads_daily_p' || to_char(m, 'YYYYMMDD'),
      m,
      (m + interval '1 month')::date
    );
  END LOOP;
END
$$;

-- ============================================================
-- pg_partman setup for downloads_last_30d (yearly partitions)
-- ============================================================
SELECT partman.create_parent(
  p_parent_table => 'public.downloads_last_30d',
  p_control      => 'end_date',
  p_interval     => '1 year',
  p_premake      => 3
);

-- Create all historical yearly partitions (2015 through last year).
DO $$
DECLARE
  y date;
BEGIN
  FOR y IN
    SELECT d::date
      FROM generate_series(
        '2015-01-01'::date,
        (date_trunc('year', now()) - interval '1 year')::date,
        '1 year'::interval
      ) AS d
  LOOP
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS %I PARTITION OF downloads_last_30d FOR VALUES FROM (%L) TO (%L)',
      'downloads_last_30d_p' || to_char(y, 'YYYYMMDD'),
      y,
      (y + interval '1 year')::date
    );
  END LOOP;
END
$$;
