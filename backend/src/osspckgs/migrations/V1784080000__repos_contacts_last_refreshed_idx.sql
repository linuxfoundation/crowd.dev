-- Supports fetchBatch's eligibility WHERE clause (contacts_last_refreshed IS NULL,
-- and the daily/weekly age comparisons), which previously had no supporting index.
CREATE INDEX IF NOT EXISTS repos_contacts_last_refreshed_idx
    ON repos (contacts_last_refreshed);
