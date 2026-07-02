ALTER TABLE security_contacts ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS security_contacts_repo_active_idx
   ON security_contacts (repo_id)
   WHERE deleted_at IS NULL;