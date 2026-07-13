-- Add the showAggregateTabs column to the collections table
ALTER TABLE collections
    ADD COLUMN "showAggregateTabs" boolean DEFAULT true NOT NULL;
