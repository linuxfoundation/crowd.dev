ALTER TABLE public.repositories DROP COLUMN license;
ALTER TABLE public.repositories ADD COLUMN licenses VARCHAR(255)[];
