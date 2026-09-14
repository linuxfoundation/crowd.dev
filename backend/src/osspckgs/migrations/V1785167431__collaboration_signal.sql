ALTER TABLE repo_activity_snapshot ADD COLUMN IF NOT EXISTS external_prs_opened_12m int;
ALTER TABLE repo_activity_snapshot ADD COLUMN IF NOT EXISTS external_prs_merged_12m int;

ALTER TABLE repos ADD COLUMN IF NOT EXISTS collaboration_score int;
ALTER TABLE repos ADD COLUMN IF NOT EXISTS collaboration_tier text;
