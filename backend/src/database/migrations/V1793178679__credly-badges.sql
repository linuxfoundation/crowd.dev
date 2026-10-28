CREATE TABLE IF NOT EXISTS integration."credlyBadges" (
    "badgeId"            TEXT PRIMARY KEY,
    "badgeTemplateId"    TEXT NOT NULL,
    "state"              TEXT NOT NULL
                         CHECK ("state" IN ('pending', 'accepted', 'rejected', 'revoked', 'replaced')),
    "stateUpdatedAt"     TIMESTAMPTZ NULL,
    "issuedAt"           TIMESTAMPTZ NOT NULL,
    "expiresAt"          TIMESTAMPTZ NULL,
    "replacementBadgeId" TEXT NULL,
    "replacesBadgeId"    TEXT NULL,
    "segmentId"          UUID NOT NULL REFERENCES public.segments(id),
    "integrationId"      UUID NOT NULL REFERENCES public.integrations(id),
    "recipientEmail"     TEXT NOT NULL,
    "issuedTo"           TEXT NULL,
    "expiredEmittedAt"   TIMESTAMPTZ NULL,
    "firstSeenAt"        TIMESTAMPTZ NOT NULL DEFAULT now(),
    "lastSeenAt"         TIMESTAMPTZ NOT NULL DEFAULT now(),
    "createdAt"          TIMESTAMPTZ NOT NULL DEFAULT now(),
    "updatedAt"          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS "ix_credlyBadges_dueExpiry"
    ON integration."credlyBadges" ("expiresAt")
    WHERE "state" = 'accepted' AND "expiredEmittedAt" IS NULL AND "expiresAt" IS NOT NULL;

CREATE INDEX IF NOT EXISTS "ix_credlyBadges_segmentId"
    ON integration."credlyBadges" ("segmentId");

CREATE INDEX IF NOT EXISTS "ix_credlyBadges_integrationId"
    ON integration."credlyBadges" ("integrationId");
