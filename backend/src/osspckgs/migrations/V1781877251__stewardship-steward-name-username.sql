ALTER TABLE stewardship_stewards
    ADD COLUMN IF NOT EXISTS username     TEXT,
    ADD COLUMN IF NOT EXISTS display_name TEXT;
