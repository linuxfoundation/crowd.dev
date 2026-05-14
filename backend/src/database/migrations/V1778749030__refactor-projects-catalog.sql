-- Drop evaluatedProjects table (data migrated into projectCatalog)
DROP TABLE IF EXISTS "evaluatedProjects";

-- Remove ossfCriticalityScore from projectCatalog
ALTER TABLE "projectCatalog" DROP COLUMN IF EXISTS "ossfCriticalityScore";
DROP INDEX IF EXISTS "ix_projectCatalog_ossfCriticalityScore";

-- Add new columns to projectCatalog
ALTER TABLE "projectCatalog"
    ADD COLUMN IF NOT EXISTS "source" VARCHAR(64),
    ADD COLUMN IF NOT EXISTS "action" VARCHAR(16) NOT NULL DEFAULT 'auto',
    ADD COLUMN IF NOT EXISTS "evaluatedAt" TIMESTAMP WITH TIME ZONE,
    ADD COLUMN IF NOT EXISTS "onboardedAt" TIMESTAMP WITH TIME ZONE;

CREATE INDEX "ix_projectCatalog_source" ON "projectCatalog" ("source");
CREATE INDEX "ix_projectCatalog_action" ON "projectCatalog" ("action");
