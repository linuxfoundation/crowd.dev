ALTER TABLE integration."credlyTemplateMappings"
    ALTER COLUMN "integrationId" DROP NOT NULL,
    ALTER COLUMN "segmentId" DROP NOT NULL,
    ADD COLUMN "status"      TEXT NOT NULL DEFAULT 'mapped',
    ADD COLUMN "holdReason"  TEXT NULL,
    ADD COLUMN "projectSlug" TEXT NULL,
    ADD COLUMN "badgeCount"  INTEGER NULL,
    ADD COLUMN "rule"        TEXT NULL,
    ADD COLUMN "ruleVersion" INTEGER NULL,
    ADD COLUMN "matchedSku"  TEXT NULL,
    ADD COLUMN "evidence"    JSONB NULL,
    ADD COLUMN "reviewedBy"  TEXT NULL,
    ADD COLUMN "reviewedAt"  TIMESTAMPTZ NULL,
    ADD COLUMN "lockedAt"    TIMESTAMPTZ NULL,
    ADD CONSTRAINT "credlyTemplateMappings_status_check"
        CHECK ("status" IN ('mapped', 'held', 'excluded')),
    ADD CONSTRAINT "credlyTemplateMappings_holdReason_check"
        CHECK ("holdReason" IS NULL
               OR "holdReason" IN ('no_segment', 'no_project', 'ambiguous', 'no_match', 'test')),
    ADD CONSTRAINT "credlyTemplateMappings_target_check"
        CHECK (("status" = 'mapped') = ("integrationId" IS NOT NULL AND "segmentId" IS NOT NULL)),
    ADD CONSTRAINT "credlyTemplateMappings_held_reason_check"
        CHECK (("status" = 'held') = ("holdReason" IS NOT NULL));
