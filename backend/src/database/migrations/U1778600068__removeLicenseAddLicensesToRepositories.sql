ALTER TABLE public.repositories DROP COLUMN licenses;
ALTER TABLE public.repositories ADD COLUMN license VARCHAR(255);
