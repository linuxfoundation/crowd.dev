DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'sequin_pub'
    ) THEN
        CREATE PUBLICATION sequin_pub
            FOR TABLE
                packages,
                versions,
                package_dependencies,
                package_maintainers,
                package_repos,
                maintainers,
                repos,
                repo_scorecard_checks,
                advisories,
                advisory_packages,
                advisory_affected_ranges
            WITH (publish_via_partition_root = true);
    END IF;
END$$;

ALTER TABLE public.packages                 REPLICA IDENTITY FULL;
ALTER TABLE public.versions                 REPLICA IDENTITY FULL;
ALTER TABLE public.package_dependencies     REPLICA IDENTITY FULL;
ALTER TABLE public.package_maintainers      REPLICA IDENTITY FULL;
ALTER TABLE public.package_repos            REPLICA IDENTITY FULL;
ALTER TABLE public.maintainers              REPLICA IDENTITY FULL;
ALTER TABLE public.repos                    REPLICA IDENTITY FULL;
ALTER TABLE public.repo_scorecard_checks    REPLICA IDENTITY FULL;
ALTER TABLE public.advisories               REPLICA IDENTITY FULL;
ALTER TABLE public.advisory_packages        REPLICA IDENTITY FULL;
ALTER TABLE public.advisory_affected_ranges REPLICA IDENTITY FULL;

-- versions (32) and package_dependencies (64) are hash-partitioned. REPLICA
-- IDENTITY on the partitioned root does not cascade; set it on every leaf.
DO $$
DECLARE
    parent_table  text;
    partition_oid regclass;
BEGIN
    FOREACH parent_table IN ARRAY ARRAY['public.versions', 'public.package_dependencies']
    LOOP
        FOR partition_oid IN
            SELECT inhrelid::regclass
            FROM pg_inherits
            WHERE inhparent = parent_table::regclass
        LOOP
            EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL', partition_oid);
        END LOOP;
    END LOOP;
END$$;
