-- Remove foreign key constraints from memberSegmentsAgg and organizationSegmentsAgg tables

-- Remove foreign key constraint from memberSegmentsAgg table
ALTER TABLE public."memberSegmentsAgg"
  DROP CONSTRAINT IF EXISTS "memberSegmentsAgg_memberId_fkey";

-- Remove foreign key constraint from organizationSegmentsAgg table
ALTER TABLE public."organizationSegmentsAgg"
  DROP CONSTRAINT IF EXISTS "organizationSegmentsAgg_organizationId_fkey";

-- Create table to track orphan cleanup operations
CREATE TABLE IF NOT EXISTS public."orphanSegmentAggsCleanupRuns" (
  "id" uuid DEFAULT uuid_generate_v4() NOT NULL,
  "tableName" VARCHAR(255) NOT NULL,
  "startedAt" TIMESTAMPTZ NOT NULL,
  "completedAt" TIMESTAMPTZ,
  "status" VARCHAR(50) NOT NULL, -- 'running', 'completed', 'failed'
  "orphansFound" INTEGER DEFAULT 0,
  "orphansDeleted" INTEGER DEFAULT 0,
  "executionTimeMs" INTEGER,
  "errorMessage" TEXT,
  "createdAt" TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  
  CONSTRAINT "orphanSegmentAggsCleanupRuns_pkey" PRIMARY KEY ("id")
);
