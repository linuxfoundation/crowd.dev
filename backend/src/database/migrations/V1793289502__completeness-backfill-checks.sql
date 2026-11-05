CREATE TABLE IF NOT EXISTS integration.completeness_backfill_checks (
  "integrationId" UUID NOT NULL,
  "channelId"     TEXT NOT NULL,
  "readyAt"       TIMESTAMPTZ,
  "createdAt"     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY ("integrationId", "channelId")
);
