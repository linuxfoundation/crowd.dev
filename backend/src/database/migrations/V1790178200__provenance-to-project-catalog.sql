ALTER TABLE public."projectCatalog"
    ADD COLUMN IF NOT EXISTS "provenance" TEXT;

ALTER TABLE public."projectCatalog"
    DROP CONSTRAINT IF EXISTS "projectCatalog_provenance_check";

ALTER TABLE public."projectCatalog"
    ADD CONSTRAINT "projectCatalog_provenance_check"
    CHECK ("provenance" IN ('github-discussion', 'slack-tag', 'lf-criticality-score'));

CREATE INDEX IF NOT EXISTS "ix_projectCatalog_provenance"
    ON public."projectCatalog" ("provenance")
    WHERE "provenance" IS NOT NULL;

UPDATE public."projectCatalog"
SET "provenance" = 'github-discussion'
WHERE "source" = 'insights-discussions' AND "provenance" IS NULL;

UPDATE public."projectCatalog"
SET "provenance" = 'lf-criticality-score'
WHERE "source" = 'lf-criticality-score' AND "provenance" IS NULL;
