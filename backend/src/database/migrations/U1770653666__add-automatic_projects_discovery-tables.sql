DROP INDEX IF EXISTS "ix_evaluatedProjects_onboarded";
DROP INDEX IF EXISTS "ix_evaluatedProjects_evaluationScore";
DROP INDEX IF EXISTS "ix_evaluatedProjects_evaluationStatus";
DROP INDEX IF EXISTS "uix_evaluatedProjects_projectCatalogId";
DROP TABLE IF EXISTS "evaluatedProjects";

DROP INDEX IF EXISTS "ix_projectCatalog_syncedAt";
DROP INDEX IF EXISTS "ix_projectCatalog_criticalityScore";
DROP INDEX IF EXISTS "uix_projectCatalog_repoUrl";
DROP TABLE IF EXISTS "projectCatalog";
