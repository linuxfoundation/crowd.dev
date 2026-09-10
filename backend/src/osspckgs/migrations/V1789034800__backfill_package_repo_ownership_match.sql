-- Backfill for CM-1394's ownership-evidence signal.
--
-- ownership_match defaulted every existing row to 'no_evidence' when the column was
-- added (V1788393600). matchOwnership() only recomputes it inside each ecosystem's
-- own upsert path, so rows never re-ingested since the CM-1394 rollout (most of the
-- table — several ecosystems only re-ingest their `is_critical` subset) stay stuck
-- on that default forever, not just until the next incremental run.
--
-- This adds the namespace segmentation matchOwnership() does in TS (ownershipMatch.ts
-- namespaceCandidates()) as a SQL-callable function, plus a chunked backfill procedure
-- that recomputes ownership_match for 'declared' rows still on the default, using
-- package_repo_owner_match() (V1788393600) against data already in packages/repos —
-- no re-crawl needed. Scoped to source='declared': deps_dev/heuristic rows never call
-- matchOwnership() by design and are left alone. Follows the chunked-procedure shape
-- of rescore_package_repo_confidence (V1788307300).

-- Mirrors ownershipMatch.ts STRUCTURAL_SEGMENTS. Keep both lists in sync.
CREATE OR REPLACE FUNCTION package_repo_structural_segments()
RETURNS text[]
LANGUAGE sql IMMUTABLE AS $$
    SELECT ARRAY[
        'com', 'org', 'net', 'io', 'dev', 'app', 'co',
        'uk', 'au', 'us', 'de', 'fr', 'in', 'jp', 'cn', 'br', 'eu', 'ru', 'nl', 'it',
        'es', 'pl', 'se', 'nz', 'za', 'mx', 'ar',
        'github', 'gitlab', 'bitbucket', 'sourceforge', 'codeberg'
    ];
$$;

-- Mirrors ownershipMatch.ts namespaceCandidates(): split a registry namespace on '.'/'/',
-- drop a leading structural segment (reverse-DNS TLD, e.g. Maven's `org.apache.commons`),
-- and drop any remaining structural segment (a VCS hostname embedded in the namespace).
-- Single-segment namespaces (npm scopes, flat vendors) pass through unchanged.
CREATE OR REPLACE FUNCTION package_repo_namespace_candidates(p_namespace text)
RETURNS text[]
LANGUAGE sql IMMUTABLE AS $$
    WITH segs AS (
        SELECT ARRAY(
            SELECT s FROM unnest(regexp_split_to_array(COALESCE(p_namespace, ''), '[./]')) AS s
             WHERE s <> ''
        ) AS all_segs
    ),
    scoped AS (
        SELECT CASE
            WHEN array_length(all_segs, 1) IS NULL OR array_length(all_segs, 1) = 1 THEN all_segs
            WHEN package_repo_owner_key(all_segs[1]) = ANY(package_repo_structural_segments())
                THEN all_segs[2 : array_length(all_segs, 1)]
            ELSE all_segs
        END AS kept
        FROM segs
    )
    SELECT COALESCE(ARRAY(
        SELECT seg FROM unnest((SELECT kept FROM scoped)) AS seg
         WHERE NOT (package_repo_owner_key(seg) = ANY(package_repo_structural_segments()))
    ), ARRAY[]::text[]);
$$;

CREATE OR REPLACE PROCEDURE backfill_package_repo_ownership_match(
    chunk_size   int DEFAULT 25000,
    INOUT applied_rows int DEFAULT 0
)
LANGUAGE plpgsql AS $$
DECLARE
    batch_rows   int;
    updated_rows int;
    cursor_id    bigint := 0;
BEGIN
    IF chunk_size IS NULL OR chunk_size <= 0 THEN
        RAISE EXCEPTION 'backfill_package_repo_ownership_match: chunk_size must be positive, got %', chunk_size;
    END IF;

    -- Session-level: survives the internal COMMITs below.
    IF NOT pg_try_advisory_lock(hashtextextended('backfill_package_repo_ownership_match', 0)) THEN
        RAISE EXCEPTION 'backfill_package_repo_ownership_match: another execution is already in progress';
    END IF;

    applied_rows := 0;

    LOOP
            WITH batch AS (
                SELECT pr.id
                  FROM package_repos pr
                 WHERE pr.id > cursor_id
                   AND pr.source = 'declared'
                   AND pr.ownership_match = 'no_evidence'
                 ORDER BY pr.id
                 LIMIT chunk_size
                   FOR UPDATE
            ),
            candidates AS (
                -- host='other' repo URLs (svn/gitbox/apache-style, not github/gitlab/bitbucket)
                -- don't carry a reliable owner segment; repoOwnerFromCanonical() in ownershipMatch.ts
                -- returns NULL for these at ingest time so they resolve to no_evidence, never a false
                -- 'unmatched'. Mirror that here instead of trusting repos.owner for them.
                SELECT b.id,
                       CASE WHEN r.host = 'other' THEN NULL ELSE r.owner END AS repo_owner,
                       package_repo_namespace_candidates(p.namespace)
                         || COALESCE(ARRAY(
                              SELECT m.username
                                FROM package_maintainers pm
                                JOIN maintainers m ON m.id = pm.maintainer_id
                               WHERE pm.package_id = cur.package_id
                                 AND pm.role = 'maintainer'
                                 AND m.username NOT LIKE '%@%'
                            ), ARRAY[]::text[]) AS owner_candidates
                  FROM batch b
                  JOIN package_repos cur ON cur.id = b.id
                  JOIN packages p ON p.id = cur.package_id
                  JOIN repos r ON r.id = cur.repo_id
            ),
            updated AS (
                UPDATE package_repos pr
                   SET ownership_match = package_repo_owner_match(c.repo_owner, c.owner_candidates)
                  FROM candidates c
                 WHERE pr.id = c.id
                   AND package_repo_owner_match(c.repo_owner, c.owner_candidates) IS DISTINCT FROM 'no_evidence'
                RETURNING pr.id
            )
            SELECT COUNT(*), COALESCE(MAX(b.id), cursor_id),
                   (SELECT COUNT(*) FROM updated)
              INTO batch_rows, cursor_id, updated_rows
              FROM batch b;

            applied_rows := applied_rows + updated_rows;

            COMMIT;

            EXIT WHEN batch_rows < chunk_size;
        END LOOP;

    PERFORM pg_advisory_unlock(hashtextextended('backfill_package_repo_ownership_match', 0));
END;
$$;
