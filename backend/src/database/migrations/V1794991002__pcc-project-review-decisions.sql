CREATE TABLE IF NOT EXISTS pcc_project_review_decisions (
  pcc_project_id       TEXT PRIMARY KEY,
  decision             TEXT NOT NULL CHECK (decision IN ('create_new', 'ignore')),
  reviewed_segment_ids UUID[] NOT NULL DEFAULT '{}',
  note                 TEXT,
  decided_by           TEXT NOT NULL,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT pcc_project_review_decisions_reviewed_segments_check
    CHECK (decision = 'ignore' OR cardinality(reviewed_segment_ids) > 0)
);
