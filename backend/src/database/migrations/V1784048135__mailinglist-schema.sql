-- Create the mailinglist schema
CREATE SCHEMA IF NOT EXISTS mailinglist;

-- Mailing lists (lore/public-inbox lists onboarded into CDP)
CREATE TABLE mailinglist.lists (
    id UUID PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4(),
    "createdAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    "updatedAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    "deletedAt" TIMESTAMP WITH TIME ZONE,

    name TEXT NOT NULL,
    "sourceUrl" TEXT NOT NULL,

    "segmentId" UUID NOT NULL REFERENCES segments (id),
    "integrationId" UUID NOT NULL REFERENCES public."integrations" (id),

    UNIQUE ("sourceUrl")
);

-- Per-list processing state (public-inbox shard head tracking)
CREATE TABLE mailinglist."listProcessing" (
    "listId" UUID PRIMARY KEY NOT NULL REFERENCES mailinglist.lists (id) ON DELETE CASCADE,

    "createdAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    "updatedAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    state VARCHAR(50) NOT NULL,
    priority INTEGER NOT NULL DEFAULT 2, -- 0=urgent, 1=high, 2=normal

    "lockedAt" TIMESTAMP WITH TIME ZONE,
    "lastProcessedAt" TIMESTAMP WITH TIME ZONE,
    "lastProcessedHeads" JSONB NOT NULL DEFAULT '{}'
);

-- Indexes for optimal query performance

CREATE INDEX "ix_mailinglist_lists_segmentId" ON mailinglist.lists ("segmentId");
CREATE INDEX "ix_mailinglist_lists_integrationId" ON mailinglist.lists ("integrationId");

CREATE INDEX "ix_mailinglist_listProcessing_state" ON mailinglist."listProcessing" (state);
CREATE INDEX "ix_mailinglist_listProcessing_state_priority" ON mailinglist."listProcessing" (state, priority);

-- Comments for documentation
COMMENT ON SCHEMA mailinglist IS 'Schema for mailing list integration system that manages mailing list processing state';
COMMENT ON TABLE mailinglist.lists IS 'Stores onboarded mailing lists (public-inbox/lore) and their segment/integration associations';
COMMENT ON TABLE mailinglist."listProcessing" IS 'Per-list processing state including public-inbox shard head tracking';

COMMENT ON COLUMN mailinglist."listProcessing".priority IS 'Processing priority: 0=urgent, 1=high, 2=normal';
COMMENT ON COLUMN mailinglist."listProcessing".state IS 'Current processing state of the list';
COMMENT ON COLUMN mailinglist."listProcessing"."lastProcessedHeads" IS 'JSONB map of shard index to last processed commit SHA, e.g. {"0": sha, "1": sha}';
