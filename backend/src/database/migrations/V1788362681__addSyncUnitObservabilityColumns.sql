ALTER TABLE integration.sync_units
  ADD COLUMN "lastErrorMessage" TEXT,
  ADD COLUMN "lastRunComplete" BOOLEAN;
