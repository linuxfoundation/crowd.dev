-- Stewardship tables for the OSSPREY Self Serve program (v1).
-- In v1: only `stewardships` is populated (one unassigned row per critical package).
-- All other tables are schema-only — empty until v2 write flows land.

CREATE TABLE IF NOT EXISTS stewardships (
    id              BIGSERIAL PRIMARY KEY,
    package_id      BIGINT NOT NULL REFERENCES packages(id),
    status          TEXT NOT NULL,      -- 'unassigned'|'open'|'assessing'|'active'|'needs_attention'|'escalated'|'blocked'|'inactive'
    origin          TEXT NOT NULL,      -- 'auto_imported'|'self_claimed'|'assigned'|'opened_for_claim'
    version         INT NOT NULL DEFAULT 1,
    opened_at       TIMESTAMPTZ,
    last_status_at  TIMESTAMPTZ,
    inactive_reason TEXT,               -- 'quarterly_cadence_missed'|'stepped_down'|'no_longer_critical'
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (package_id)
);

CREATE INDEX IF NOT EXISTS stewardships_status_idx
    ON stewardships (status);
CREATE INDEX IF NOT EXISTS stewardships_last_status_at_active_idx
    ON stewardships (last_status_at) WHERE status = 'active';

-- Many-to-many stewards. Empty in v1; soft-delete preserves historical membership.
CREATE TABLE IF NOT EXISTS stewardship_stewards (
    id              BIGSERIAL PRIMARY KEY,
    stewardship_id  BIGINT NOT NULL REFERENCES stewardships(id),
    user_id         TEXT NOT NULL,
    role            TEXT NOT NULL,      -- 'lead'|'co_steward'
    assigned_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    assigned_by     TEXT,
    deleted_at      TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS stewardship_stewards_active_unique
    ON stewardship_stewards (stewardship_id, user_id)
    WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS stewardship_stewards_user_id_active_idx
    ON stewardship_stewards (user_id) WHERE deleted_at IS NULL;

-- Append-only audit log. Empty in v1.
CREATE TABLE IF NOT EXISTS stewardship_activity (
    id              BIGSERIAL PRIMARY KEY,
    stewardship_id  BIGINT NOT NULL REFERENCES stewardships(id),
    actor_user_id   TEXT,               -- NULL for system events
    actor_type      TEXT NOT NULL,      -- 'user'|'system'
    activity_type   TEXT NOT NULL,      -- 'state_changed'|'assessment_completed'|'assessment_flagged'|
                                        -- 'remediation_logged'|'status_update'|'escalation'|
                                        -- 'escalation_resolved'|'blocker_added'|'blocker_resolved'|
                                        -- 'steward_added'|'steward_removed'
    content         TEXT,
    metadata        JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS stewardship_activity_stewardship_id_created_at_idx
    ON stewardship_activity (stewardship_id, created_at DESC);

-- One current assessment per stewardship; historical ones preserved via superseded_at.
CREATE TABLE IF NOT EXISTS stewardship_assessments (
    id                    BIGSERIAL PRIMARY KEY,
    stewardship_id        BIGINT NOT NULL REFERENCES stewardships(id),
    posture               TEXT,
    summary               TEXT,
    security_contact      TEXT,
    disclosure_preference TEXT,
    tier_0_ready          BOOL NOT NULL DEFAULT FALSE,
    monitoring_plan       TEXT,
    draft                 BOOL NOT NULL DEFAULT TRUE,
    completed_at          TIMESTAMPTZ,
    completed_by          TEXT,
    reviewed              BOOL NOT NULL DEFAULT FALSE,
    reviewed_at           TIMESTAMPTZ,
    reviewed_by           TEXT,
    flagged               BOOL NOT NULL DEFAULT FALSE,
    flag_note             TEXT,
    superseded_at         TIMESTAMPTZ,
    superseded_by_id      BIGINT REFERENCES stewardship_assessments(id),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()   -- tracks mutations: reviewed, flagged, superseded_at
);

CREATE INDEX IF NOT EXISTS stewardship_assessments_stewardship_id_superseded_at_idx
    ON stewardship_assessments (stewardship_id, superseded_at);
CREATE UNIQUE INDEX IF NOT EXISTS stewardship_assessments_one_current
    ON stewardship_assessments (stewardship_id)
    WHERE superseded_at IS NULL;

-- Per-dimension findings. assessment_id links a finding to the assessment that produced it.
CREATE TABLE IF NOT EXISTS stewardship_findings (
    id              BIGSERIAL PRIMARY KEY,
    stewardship_id  BIGINT NOT NULL REFERENCES stewardships(id),
    assessment_id   BIGINT REFERENCES stewardship_assessments(id),  -- NULL until assessment flow lands in v2
    dimension       TEXT NOT NULL,      -- 'maintainer_health'|'security_posture'|'vulnerability_exposure'|
                                        -- 'dependency_risk'|'supply_chain_integrity'|'release_health'
    severity        TEXT NOT NULL,      -- 'critical'|'high'|'medium'|'low'|'informational'
    finding         TEXT NOT NULL,
    evidence        TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS stewardship_findings_stewardship_id_idx
    ON stewardship_findings (stewardship_id);
CREATE INDEX IF NOT EXISTS stewardship_findings_dimension_severity_idx
    ON stewardship_findings (dimension, severity);

-- Concrete remediation actions. Empty in v1.
CREATE TABLE IF NOT EXISTS stewardship_remediation_actions (
    id              BIGSERIAL PRIMARY KEY,
    stewardship_id  BIGINT NOT NULL REFERENCES stewardships(id),
    finding_id      BIGINT REFERENCES stewardship_findings(id),
    action          TEXT NOT NULL,
    status          TEXT NOT NULL,      -- 'pending'|'in_progress'|'done'|'blocked'|'abandoned'
    url             TEXT,
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS stewardship_remediation_actions_stewardship_id_status_idx
    ON stewardship_remediation_actions (stewardship_id, status);
