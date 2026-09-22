ALTER TABLE "projectCatalog"
  ADD COLUMN IF NOT EXISTS "evaluationResult" TEXT,
  ADD COLUMN IF NOT EXISTS "evaluationReason" TEXT;
