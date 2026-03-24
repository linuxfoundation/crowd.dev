
ALTER TABLE "maintainersInternal"
DROP CONSTRAINT IF EXISTS "maintainersInternal_repoId_fkey";

ALTER TABLE "maintainersInternal"
ADD CONSTRAINT "maintainersInternal_repoId_fkey"
FOREIGN KEY ("repoId")
REFERENCES repositories(id)
ON DELETE CASCADE;