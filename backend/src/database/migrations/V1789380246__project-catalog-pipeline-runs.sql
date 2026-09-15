-- One row per stage per run; no run id is shared across the three pipeline stages.
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

-- Lets the DAL upsert on (stage, temporalRunId) so a retried activity can't double-write a run.
CREATE UNIQUE INDEX "ux_projectCatalogPipelineRuns_stage_temporalRunId"
    ON public."projectCatalogPipelineRuns" ("stage", "temporalRunId")
    WHERE "temporalRunId" IS NOT NULL;
