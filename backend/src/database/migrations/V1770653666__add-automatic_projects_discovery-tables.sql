-- Project Catalog: candidate projects discovered from OSSF Criticality Score and other sources
CREATE TABLE IF NOT EXISTS "projectCatalog" (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "projectSlug" VARCHAR(255) NOT NULL,
    "repoName" VARCHAR(255) NOT NULL,
    "repoUrl" VARCHAR(1024) NOT NULL,
    "criticalityScore" DOUBLE PRECISION,
    "syncedAt" TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    "createdAt" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX "uix_projectCatalog_repoUrl" ON "projectCatalog" ("repoUrl");
CREATE INDEX "ix_projectCatalog_criticalityScore" ON "projectCatalog" ("criticalityScore" DESC NULLS LAST);
CREATE INDEX "ix_projectCatalog_syncedAt" ON "projectCatalog" ("syncedAt");

-- Evaluated Projects: AI evaluation results linked to catalog entries
CREATE TABLE IF NOT EXISTS "evaluatedProjects" (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "projectCatalogId" UUID NOT NULL REFERENCES "projectCatalog"(id) ON DELETE CASCADE,
    "evaluationStatus" VARCHAR(50) NOT NULL DEFAULT 'pending',
    "evaluationScore" DOUBLE PRECISION,
    "evaluation" JSONB,
    "evaluationReason" TEXT,
    "evaluatedAt" TIMESTAMP WITH TIME ZONE,
    "starsCount" INTEGER,
    "forksCount" INTEGER,
    "commitsCount" INTEGER,
    "pullRequestsCount" INTEGER,
    "issuesCount" INTEGER,
    "onboarded" BOOLEAN NOT NULL DEFAULT FALSE,
    "onboardedAt" TIMESTAMP WITH TIME ZONE,
    "createdAt" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX "uix_evaluatedProjects_projectCatalogId" ON "evaluatedProjects" ("projectCatalogId");
CREATE INDEX "ix_evaluatedProjects_evaluationStatus" ON "evaluatedProjects" ("evaluationStatus");
CREATE INDEX "ix_evaluatedProjects_evaluationScore" ON "evaluatedProjects" ("evaluationScore" DESC NULLS LAST);
CREATE INDEX "ix_evaluatedProjects_onboarded" ON "evaluatedProjects" ("onboarded");
