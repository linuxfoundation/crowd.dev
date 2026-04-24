ALTER TABLE "collectionsRepositories" REPLICA IDENTITY FULL;
GRANT SELECT ON "collectionsRepositories" TO sequin;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'sequin_pub' AND tablename = 'collectionsRepositories'
    ) THEN
        ALTER PUBLICATION sequin_pub ADD TABLE "collectionsRepositories";
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS "ix_collectionsRepositories_updatedAt_id" ON "collectionsRepositories" ("updatedAt", id);
