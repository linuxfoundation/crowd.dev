ALTER TABLE integration."snowflakeExportJobs" ADD COLUMN "sourceName" VARCHAR(100);

UPDATE integration."snowflakeExportJobs" SET "sourceName" = 'event-registrations' WHERE platform = 'cvent';

ALTER TABLE integration."snowflakeExportJobs" ALTER COLUMN "sourceName" SET NOT NULL;

CREATE INDEX "idx_snowflakeExportJobs_platform_source" ON integration."snowflakeExportJobs" (platform, "sourceName");
