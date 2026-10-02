CREATE TABLE IF NOT EXISTS integration."octolensKeywordMappings" (
    id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    "integrationId"   UUID NOT NULL REFERENCES public.integrations(id) ON DELETE CASCADE,
    "segmentId"       UUID NOT NULL REFERENCES public.segments(id),
    "keywordId"       INTEGER NOT NULL,
    keyword           TEXT NOT NULL,
    "createdAt"       TIMESTAMPTZ NOT NULL DEFAULT now(),
    "updatedAt"       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE ("integrationId", "keywordId")
);
