ALTER TABLE public."repositoryStarBackfillFailures"
    RENAME TO "repositoryStarBackfillStatus";

ALTER INDEX "ix_repositoryStarBackfillFailures_deadLetteredAt"
    RENAME TO "ix_repositoryStarBackfillStatus_deadLetteredAt";

ALTER TABLE public."repositoryStarBackfillStatus"
    ADD COLUMN "completedAt" TIMESTAMP WITH TIME ZONE;
