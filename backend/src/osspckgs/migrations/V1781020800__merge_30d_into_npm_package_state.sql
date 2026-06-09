-- ── 1. Add the 30d watermark columns to npm_package_state ──────────────────────
ALTER TABLE npm_package_state
  ADD COLUMN IF NOT EXISTS downloads_30d_last_run_at           timestamptz,  -- breadth watermark: latest 30d window refreshed
  ADD COLUMN IF NOT EXISTS downloads_30d_history_backfilled_at timestamptz,  -- depth watermark: NULL until full older history filled
  ADD COLUMN IF NOT EXISTS downloads_30d_run_result            jsonb;        -- { status, httpStatus?, errorKind?, message? }

-- Recreate the two indexes the old table had — both due-selection queries
-- filter/order on these columns.
CREATE INDEX IF NOT EXISTS npm_package_state_downloads_30d_last_run_at_idx
  ON npm_package_state (downloads_30d_last_run_at);
CREATE INDEX IF NOT EXISTS npm_package_state_downloads_30d_history_backfilled_at_idx
  ON npm_package_state (downloads_30d_history_backfilled_at);

-- ── 2. Migrate existing rows ───────────────────────────────────────────────────
INSERT INTO npm_package_state
  (purl, downloads_30d_last_run_at, downloads_30d_history_backfilled_at, downloads_30d_run_result)
SELECT purl, downloads_30d_last_run_at, downloads_30d_history_backfilled_at, downloads_30d_run_result
  FROM npm_package_universe_state
ON CONFLICT (purl) DO UPDATE SET
  downloads_30d_last_run_at           = EXCLUDED.downloads_30d_last_run_at,
  downloads_30d_history_backfilled_at = EXCLUDED.downloads_30d_history_backfilled_at,
  downloads_30d_run_result            = EXCLUDED.downloads_30d_run_result;

-- ── 3. Drop the retired table ──────────────────────────────────────────────────
DROP TABLE npm_package_universe_state;
