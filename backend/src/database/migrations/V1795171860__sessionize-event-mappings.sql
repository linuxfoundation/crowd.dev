CREATE TABLE IF NOT EXISTS integration."sessionizeEventMappings" (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    "eventKey"      TEXT NOT NULL UNIQUE,
    "eventName"     TEXT NOT NULL,
    "eventStart"    DATE NOT NULL,
    "eventEnd"      DATE NOT NULL,
    "cfpStart"      TIMESTAMP NULL,
    "cfpEnd"        TIMESTAMP NULL,
    "status"        TEXT NOT NULL,
    "holdReason"    TEXT NULL,
    "cventEventId"  TEXT NULL,
    "segmentId"     UUID NULL REFERENCES public.segments(id),
    "integrationId" UUID NULL REFERENCES public.integrations(id) ON DELETE SET NULL,
    "rule"          TEXT NULL,
    "ruleVersion"   INTEGER NOT NULL,
    "evidence"      JSONB NOT NULL,
    "reviewedBy"    TEXT NULL,
    "reviewedAt"    TIMESTAMPTZ NULL,
    "lockedAt"      TIMESTAMPTZ NULL,
    "lastSeenAt"    TIMESTAMPTZ NOT NULL DEFAULT now(),
    "deactivatedAt" TIMESTAMPTZ NULL,
    "createdAt"     TIMESTAMPTZ NOT NULL DEFAULT now(),
    "updatedAt"     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT "sessionizeEventMappings_status_check"
        CHECK ("status" IN ('matched', 'held', 'excluded')),
    CONSTRAINT "sessionizeEventMappings_holdReason_check"
        CHECK ("holdReason" IS NULL OR "holdReason" IN ('no_match', 'ambiguous', 'no_segment')),
    CONSTRAINT "sessionizeEventMappings_target_check"
        CHECK (("status" = 'matched') = ("segmentId" IS NOT NULL)),
    CONSTRAINT "sessionizeEventMappings_held_reason_check"
        CHECK (("status" = 'held') = ("holdReason" IS NOT NULL))
);
