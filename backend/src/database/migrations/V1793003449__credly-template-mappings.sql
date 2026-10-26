CREATE TABLE IF NOT EXISTS integration."credlyTemplateMappings" (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    "badgeTemplateId"   TEXT NOT NULL UNIQUE,
    "badgeTemplateName" TEXT NOT NULL,
    "integrationId"     UUID NOT NULL REFERENCES public.integrations(id) ON DELETE CASCADE,
    "segmentId"         UUID NOT NULL REFERENCES public.segments(id),
    "deactivatedAt"     TIMESTAMPTZ NULL,
    "lastSeenAt"        TIMESTAMPTZ NOT NULL DEFAULT now(),
    "createdAt"         TIMESTAMPTZ NOT NULL DEFAULT now(),
    "updatedAt"         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS "ix_credlyTemplateMappings_integrationId"
    ON integration."credlyTemplateMappings" ("integrationId");
