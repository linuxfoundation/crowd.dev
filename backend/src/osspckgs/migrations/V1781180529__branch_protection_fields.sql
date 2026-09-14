ALTER TABLE repos ADD COLUMN IF NOT EXISTS branch_protection_enabled boolean;
ALTER TABLE repos ADD COLUMN IF NOT EXISTS branch_protection_required_reviews int;
ALTER TABLE repos ADD COLUMN IF NOT EXISTS branch_protection_requires_status_checks boolean;
ALTER TABLE repos ADD COLUMN IF NOT EXISTS branch_protection_allows_force_push boolean;
