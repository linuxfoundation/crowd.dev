-- Widens the unique index on advisory_affected_ranges from
--   (advisory_package_id, COALESCE(introduced_version, ''))
-- to the full range tuple
--   (advisory_package_id,
--    COALESCE(introduced_version, ''),
--    COALESCE(fixed_version,    ''),
--    COALESCE(last_affected,    ''))
-- so OSV records that emit multiple `affected[]` blocks for the same package
-- sharing an introduced_version but differing in fixed_version / last_affected
-- (cross-distro patches, partial fixes) no longer collide on insert and lose
-- the wider range. Restores the osv-plan §2 decision #1 invariant: "one
-- package has many version ranges, no denormalization." See ADR-0006.
DO $$
DECLARE
    idx_name text;
BEGIN
    SELECT indexname INTO idx_name
    FROM pg_indexes
    WHERE schemaname = current_schema()
      AND tablename = 'advisory_affected_ranges'
      AND indexdef ILIKE '%UNIQUE%'
      AND indexdef ILIKE '%advisory_package_id%'
      AND indexdef ILIKE '%COALESCE(introduced_version%'
      AND indexdef NOT ILIKE '%fixed_version%'
      AND indexdef NOT ILIKE '%last_affected%';
    IF idx_name IS NOT NULL THEN
        EXECUTE 'DROP INDEX ' || quote_ident(idx_name);
    END IF;
END $$;

CREATE UNIQUE INDEX ON advisory_affected_ranges (
    advisory_package_id,
    COALESCE(introduced_version, ''),
    COALESCE(fixed_version, ''),
    COALESCE(last_affected, '')
);
