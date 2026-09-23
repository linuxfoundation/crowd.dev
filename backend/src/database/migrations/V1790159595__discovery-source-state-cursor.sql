ALTER TABLE public."discoverySourceState"
  ADD COLUMN IF NOT EXISTS "cursor" JSONB;
