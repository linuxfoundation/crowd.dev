ALTER TABLE public."projectCatalog"
    DROP CONSTRAINT IF EXISTS "projectCatalog_provenance_check";

ALTER TABLE public."projectCatalog"
    ADD CONSTRAINT "projectCatalog_provenance_check"
    CHECK ("provenance" IN ('github-discussion', 'slack-tag', 'slack-bot', 'lf-criticality-score'));
