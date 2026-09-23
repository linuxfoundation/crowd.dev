ALTER TABLE public."projectCatalog"
  ADD COLUMN IF NOT EXISTS "sourceUrl" TEXT;
