ALTER TABLE pcc_projects_sync_errors
  ADD COLUMN IF NOT EXISTS review_decision TEXT
    CONSTRAINT pcc_projects_sync_errors_review_decision_check CHECK (review_decision IN ('ignore')),
  ADD COLUMN IF NOT EXISTS review_note TEXT,
  ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ;
