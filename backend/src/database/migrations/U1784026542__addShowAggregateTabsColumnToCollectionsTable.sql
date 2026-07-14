-- Reverses V1784026542: drops the showAggregateTabs column added to collections.
ALTER TABLE collections
    DROP COLUMN "showAggregateTabs";
