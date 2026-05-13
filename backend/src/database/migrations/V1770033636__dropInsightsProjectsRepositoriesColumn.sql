-- Drop the deprecated repositories column from insightsProjects
-- Repositories are now managed via public.repositories table
-- The API still accepts repositories param which syncs public.repositories.enabled

ALTER TABLE "insightsProjects" DROP COLUMN IF EXISTS "repositories";
