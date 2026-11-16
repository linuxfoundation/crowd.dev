ALTER TABLE public.repositories ADD COLUMN "unavailableAt" TIMESTAMPTZ;
COMMENT ON COLUMN public.repositories."unavailableAt" IS 'Set when the provider confirms the repository no longer exists or is no longer accessible; the repository stays visible on insights but is no longer ingested';
