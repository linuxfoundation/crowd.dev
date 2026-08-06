ALTER TABLE security_contacts ADD COLUMN IF NOT EXISTS reachable           BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE security_contacts ADD COLUMN IF NOT EXISTS reachability_reason TEXT;
