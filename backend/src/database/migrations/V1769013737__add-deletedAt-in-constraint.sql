-- Fix unique constraint for collectionsInsightsProjects to exclude soft-deleted rows

-- Drop the existing unique constraint
ALTER TABLE public."collectionsInsightsProjects"
DROP CONSTRAINT IF EXISTS "collectionsInsightsProjects_collectionId_insightsProjectId_key";

-- Create a partial unique index that only applies to non-deleted rows
CREATE UNIQUE INDEX IF NOT EXISTS "collectionsInsightsProjects_unique_active"
ON public."collectionsInsightsProjects" ("collectionId", "insightsProjectId")
WHERE "deletedAt" IS NULL;