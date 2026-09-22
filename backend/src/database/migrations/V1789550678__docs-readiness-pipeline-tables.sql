-- Docs readiness pipeline (IN-1305): one row per sweep or on-demand run.
CREATE TABLE public."projectDocReadinessRuns" (
    "id"             UUID PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4(),
    "trigger"        TEXT NOT NULL CHECK ("trigger" IN ('scheduled-full', 'scheduled-incremental', 'on-demand')),
    "scope"          TEXT NOT NULL CHECK ("scope" IN ('lf', 'all')),
    "status"         TEXT NOT NULL DEFAULT 'running' CHECK ("status" IN ('running', 'completed', 'failed')),
    "workflowId"     TEXT,
    "temporalRunId"  TEXT,
    "startedAt"      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    "finishedAt"     TIMESTAMP WITH TIME ZONE,
    "totalProjects"  INTEGER,
    "discovered"     INTEGER,
    "scored"         INTEGER,
    "failed"         INTEGER,
    "errorMessage"   TEXT,
    "createdAt"      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    "updatedAt"      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Lets the DAL upsert on workflowId so a retried activity can't double-write a run.
CREATE UNIQUE INDEX "ux_projectDocReadinessRuns_workflowId"
    ON public."projectDocReadinessRuns" ("workflowId")
    WHERE "workflowId" IS NOT NULL;

-- Latest documentation URL discovered per project (one row per project, overwritten on re-discovery).
CREATE TABLE public."projectDocDiscoveries" (
    "projectId"        UUID PRIMARY KEY NOT NULL REFERENCES public."insightsProjects"("id") ON DELETE CASCADE,
    "docsUrl"          TEXT,
    "discoveryMethod"  TEXT,
    "confidence"       TEXT,
    "candidates"       JSONB NOT NULL DEFAULT '[]',
    "discoveredAt"     TIMESTAMP WITH TIME ZONE NOT NULL,
    "createdAt"        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    "updatedAt"        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Manual docs URL overrides; history is kept, only one row per project may be active.
CREATE TABLE public."projectDocOverrides" (
    "id"           UUID PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4(),
    "projectId"    UUID NOT NULL REFERENCES public."insightsProjects"("id") ON DELETE CASCADE,
    "docsUrl"      TEXT NOT NULL,
    "submittedBy"  TEXT NOT NULL,
    "submittedAt"  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    "active"       BOOLEAN NOT NULL DEFAULT TRUE,
    "createdAt"    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    "updatedAt"    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX "ux_projectDocOverrides_projectId_active"
    ON public."projectDocOverrides" ("projectId")
    WHERE "active";
