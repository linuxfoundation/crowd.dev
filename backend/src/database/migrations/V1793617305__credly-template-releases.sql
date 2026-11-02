CREATE TABLE IF NOT EXISTS integration."credlyTemplateReleases" (
    "badgeTemplateId" TEXT PRIMARY KEY,
    "floor"           TIMESTAMPTZ NOT NULL,
    "top"             TIMESTAMPTZ NOT NULL,
    "since"           TIMESTAMPTZ NULL,
    "until"           TIMESTAMPTZ NULL,
    "page"            INTEGER NOT NULL DEFAULT 1,
    "queuedAt"        TIMESTAMPTZ NOT NULL DEFAULT now(),
    "completedAt"     TIMESTAMPTZ NULL,
    "createdAt"       TIMESTAMPTZ NOT NULL DEFAULT now(),
    "updatedAt"       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ("floor" < "top")
);

CREATE INDEX IF NOT EXISTS "ix_credlyTemplateReleases_pending"
    ON integration."credlyTemplateReleases" ("queuedAt")
    WHERE "completedAt" IS NULL;
