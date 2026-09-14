-- Per-run outcome ledger for the critical projects onboarding pipeline's three
-- independent stages (discovery, evaluation, onboarding). Each worker writes its
-- own row at the end of its own run — no shared run id across stages.
CREATE TABLE public."projectCatalogPipelineRuns" (
    "id"                    UUID PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4(),
    "stage"                 TEXT NOT NULL CHECK ("stage" IN ('discovery', 'evaluation', 'onboarding')),
    "status"                TEXT NOT NULL DEFAULT 'running' CHECK ("status" IN ('running', 'completed', 'failed')),
    "workflowId"            TEXT,
    "temporalRunId"         TEXT,
    "startedAt"             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    "finishedAt"            TIMESTAMP WITH TIME ZONE,
    "elapsedSeconds"        NUMERIC(12, 3),
    "totalCandidates"       INTEGER,
    "succeeded"             INTEGER,
    "failed"                INTEGER,
    "skipped"               INTEGER,
    "skippedPreCheck"       INTEGER,
    "errorMessage"          TEXT,
    "details"               JSONB,
    "evaluatorCalls"        INTEGER,
    "evaluatorInputTokens"  BIGINT,
    "evaluatorOutputTokens" BIGINT,
    "evaluatorCostUsd"      NUMERIC(12, 6),
    "evaluatorSeconds"      NUMERIC(12, 3),
    "evaluatorModels"       JSONB,
    "createdAt"             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    "updatedAt"             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX "ix_projectCatalogPipelineRuns_stage_startedAt"
    ON public."projectCatalogPipelineRuns" ("stage", "startedAt" DESC);
