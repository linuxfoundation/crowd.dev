-- Drop collectionLikes table
DROP TABLE IF EXISTS public."collectionLikes";

-- Drop collectionsRepositories table
DROP TABLE IF EXISTS public."collectionsRepositories";

-- Drop FK before dropping insightsSsoUsers
ALTER TABLE public.collections
    DROP CONSTRAINT IF EXISTS "fk_collections_ssoUserId";

-- Drop columns from collections
ALTER TABLE public.collections
    DROP COLUMN IF EXISTS "isPrivate",
    DROP COLUMN IF EXISTS "ssoUserId",
    DROP COLUMN IF EXISTS "logoUrl";

-- Drop insightsSsoUsers table
DROP TABLE IF EXISTS public."insightsSsoUsers";
