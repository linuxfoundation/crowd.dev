ALTER TABLE public.repositories ADD COLUMN enabled BOOLEAN NOT NULL DEFAULT TRUE;
COMMENT ON COLUMN public.repositories.enabled IS 'Used to enable/disable repository on insights';
