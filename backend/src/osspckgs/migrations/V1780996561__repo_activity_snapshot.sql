ALTER TABLE repos ADD COLUMN IF NOT EXISTS security_policy_enabled boolean;
ALTER TABLE repos ADD COLUMN IF NOT EXISTS security_file_enabled boolean;
ALTER TABLE repos ADD COLUMN IF NOT EXISTS snapshot_at timestamptz;

CREATE TABLE IF NOT EXISTS repo_activity_snapshot (
  repo_id                                   bigint PRIMARY KEY REFERENCES repos(id) ON DELETE CASCADE,
  snapshot_at                               timestamptz NOT NULL,
  window_months                             int NOT NULL DEFAULT 12,
  -- commit activity
  commits_last_12m                          int,
  commits_last_6m                           int,
  commits_prior_6m                          int,
  -- PR health
  prs_opened_last_12m                       int,
  prs_merged_last_12m                       int,
  prs_closed_unmerged_12m                   int,
  pr_median_time_to_merge_hours             int,
  pr_median_time_to_first_response_hours    int,
  -- issue health
  issues_opened_last_12m                    int,
  issues_closed_last_12m                    int,
  issues_opened_last_6m                     int,
  issues_opened_prior_6m                    int,
  issues_open_now                           int,
  issue_median_time_to_close_hours          int,
  issue_median_time_to_first_response_hours int
);

CREATE INDEX IF NOT EXISTS repo_activity_snapshot_snapshot_at_idx
  ON repo_activity_snapshot (snapshot_at);
