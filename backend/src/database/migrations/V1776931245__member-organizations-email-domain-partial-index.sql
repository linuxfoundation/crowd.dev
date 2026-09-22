CREATE INDEX CONCURRENTLY IF NOT EXISTS "ix_memberOrganizations_memberId_emailDomain"
  ON "memberOrganizations" ("memberId")
  WHERE "source" = 'email-domain' AND "deletedAt" IS NULL;
