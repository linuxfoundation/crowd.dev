CREATE INDEX IF NOT EXISTS ix_sync_units_emitting_channel
  ON integration.sync_units ("integrationId", lower("channelName"), "syncName")
  WHERE "emitEnabled";
