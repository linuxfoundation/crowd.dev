-- Drop the existing foreign key constraint to git.repositories
ALTER TABLE git."serviceExecutions" 
    DROP CONSTRAINT "serviceExecutions_repoId_fkey";

-- Add new foreign key constraint to public.repositories
ALTER TABLE git."serviceExecutions" 
    ADD CONSTRAINT "serviceExecutions_repoId_fkey" 
    FOREIGN KEY ("repoId") REFERENCES public.repositories(id) ON DELETE CASCADE;
