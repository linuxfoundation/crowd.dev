-- Add new columns to collections table
ALTER TABLE public.collections
    ADD COLUMN "isPrivate"  BOOLEAN      NOT NULL DEFAULT false,
    ADD COLUMN "ssoUserId"  VARCHAR(255),          -- nullable: only set for community collections
    ADD COLUMN "logoUrl"    TEXT;                  -- nullable: only set for curated collections

-- Create ssoUsers table (id = SSO subject ID, e.g. "auth0|abc123")
CREATE TABLE public."insightsSsoUsers" (
    id               VARCHAR(255) PRIMARY KEY,
    "displayName"    VARCHAR(255),
    "avatarUrl"      TEXT,
    "email"          VARCHAR(255),
    "username"       VARCHAR(255),
    "accountId"      VARCHAR(255),
    "accountName"    VARCHAR(255),
    "accountWebsite" TEXT,
    "updatedAt"      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- FK: collections.ssoUserId -> ssoUsers.id
ALTER TABLE public.collections
    ADD CONSTRAINT "fk_collections_ssoUserId"
    FOREIGN KEY ("ssoUserId") REFERENCES public."insightsSsoUsers"(id) ON DELETE SET NULL;

-- Create collectionsRepositories table
CREATE TABLE public."collectionsRepositories" (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "collectionId"  UUID NOT NULL REFERENCES public.collections(id) ON DELETE CASCADE,
    "repoId"        UUID NOT NULL REFERENCES public.repositories(id) ON DELETE CASCADE,
    "createdAt"     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    "updatedAt"     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    "deletedAt"     TIMESTAMP WITH TIME ZONE
);

CREATE INDEX "ix_collectionsRepositories_collectionId"
    ON public."collectionsRepositories" ("collectionId")
    WHERE "deletedAt" IS NULL;

-- Create collectionLikes table
CREATE TABLE public."collectionLikes" (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    "collectionId"  UUID         NOT NULL REFERENCES public.collections(id) ON DELETE CASCADE,
    "ssoUserId"     VARCHAR(255) NOT NULL REFERENCES public."insightsSsoUsers"(id) ON DELETE CASCADE,
    "createdAt"     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    "deletedAt"     TIMESTAMP WITH TIME ZONE
);

-- Partial unique index so a user can re-like after unliking (soft delete)
CREATE UNIQUE INDEX "ix_collectionLikes_unique_active"
    ON public."collectionLikes" ("collectionId", "ssoUserId")
    WHERE "deletedAt" IS NULL;

CREATE INDEX "ix_collectionLikes_collectionId"
    ON public."collectionLikes" ("collectionId")
    WHERE "deletedAt" IS NULL;
