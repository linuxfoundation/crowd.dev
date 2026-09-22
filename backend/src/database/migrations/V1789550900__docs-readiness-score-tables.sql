-- Docs readiness pipeline (IN-1305): one score row per project per run date. Replicated to Tinybird (IN-1314).
CREATE TABLE public."projectDocReadiness" (
    "id"               UUID PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4(),
    "projectId"        UUID NOT NULL REFERENCES public."insightsProjects"("id") ON DELETE CASCADE,
    "projectSlug"      TEXT NOT NULL,
    "projectName"      TEXT NOT NULL,
    "docsUrl"          TEXT,
    "discoveryMethod"  TEXT,
    "confidence"       TEXT,
    "isOverride"       BOOLEAN NOT NULL DEFAULT FALSE,
    "overallScore"     SMALLINT,
    "overallGrade"     TEXT,
    "categoryScores"   JSONB,
    "runDate"          DATE NOT NULL,
    "runId"            UUID REFERENCES public."projectDocReadinessRuns"("id") ON DELETE SET NULL,
    "durationMs"       INTEGER,
    "ok"               BOOLEAN NOT NULL,
    "error"            TEXT,
    "createdAt"        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    "updatedAt"        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE ("projectId", "runDate")
);

CREATE INDEX "ix_projectDocReadiness_projectId_runDate"
    ON public."projectDocReadiness" ("projectId", "runDate" DESC);

-- Incremental sweeps and backfills page by (updatedAt, id).
CREATE INDEX "ix_projectDocReadiness_updatedAt_id"
    ON public."projectDocReadiness" ("updatedAt", "id");

-- Latest per-check afdocs result per project; replaced wholesale on each successful scoring. Not replicated.
CREATE TABLE public."projectDocReadinessChecks" (
    "projectId"   UUID NOT NULL REFERENCES public."insightsProjects"("id") ON DELETE CASCADE,
    "checkId"     TEXT NOT NULL,
    "category"    TEXT NOT NULL,
    "status"      TEXT NOT NULL CHECK ("status" IN ('pass', 'warn', 'fail', 'skip', 'error')),
    "message"     TEXT,
    "details"     TEXT,
    "durationMs"  INTEGER,
    "scoredAt"    TIMESTAMP WITH TIME ZONE NOT NULL,
    "createdAt"   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    "updatedAt"   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY ("projectId", "checkId")
);
