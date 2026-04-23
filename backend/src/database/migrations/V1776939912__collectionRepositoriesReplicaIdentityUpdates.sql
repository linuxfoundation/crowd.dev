ALTER TABLE "collectionsRepositories" REPLICA IDENTITY FULL;
GRANT SELECT ON "collectionsRepositories" TO sequin;

CREATE INDEX "ix_collectionsRepositories_updatedAt_id" ON "collectionsRepositories" ("updatedAt", id);
