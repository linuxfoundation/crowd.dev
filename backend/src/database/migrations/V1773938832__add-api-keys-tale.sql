CREATE TABLE "apiKeys" (
  "id"          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  "name"        TEXT NOT NULL,
  "keyHash"     TEXT NOT NULL UNIQUE,
  "keyPrefix"   TEXT NOT NULL,
  "scopes"      TEXT[] NOT NULL DEFAULT '{}',
  "expiresAt"   TIMESTAMPTZ,
  "lastUsedAt"  TIMESTAMPTZ,
  "createdById" TEXT,
  "revokedAt"   TIMESTAMPTZ,
  "createdAt"   TIMESTAMPTZ NOT NULL DEFAULT now(),
  "updatedAt"   TIMESTAMPTZ NOT NULL DEFAULT now()
);
