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

-- Mirrors ownershipMatch.ts repoOwnerFromCanonical(): the first path segment after the
-- host, parsed straight from the URL. getOrCreateRepoByUrl() never populates repos.owner
-- at ingest (a later GitHub-only enricher does), so trusting that column here would leave
-- npm/pypi/packagist/nuget/go/cargo rows — and all gitlab/bitbucket rows, never enriched —
-- wrongly stuck on no_evidence. host='other'/'svn'/'gerrit' have no reliable owner segment
-- (maven/normalize.ts stores these with owner=null; their path's first segment is a fixed
-- forge artifact like 'repos' or 'gerrit', not an owner), same as ingest.
CREATE OR REPLACE FUNCTION package_repo_owner_from_url(p_host text, p_url text)
RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN p_host IN ('other', 'svn', 'gerrit') OR p_url IS NULL THEN NULL
        ELSE NULLIF((regexp_split_to_array(regexp_replace(p_url, '^https?://[^/]+/', ''), '/'))[1], '')
    END;
$$;

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
    )
    -- Single-segment namespaces pass through unfiltered, even a structural word
    -- (e.g. npm scope `@github`) — namespaceCandidates() only applies the
    -- structural-segment filter once there's more than one segment to scope.
    SELECT CASE
        WHEN array_length(all_segs, 1) IS NULL THEN ARRAY[]::text[]
        WHEN array_length(all_segs, 1) = 1 THEN all_segs
        ELSE COALESCE(ARRAY(
            SELECT seg
              FROM unnest(
                       CASE WHEN package_repo_owner_key(all_segs[1]) = ANY(package_repo_structural_segments())
                           THEN all_segs[2 : array_length(all_segs, 1)]
                           ELSE all_segs
                       END
                   ) AS seg
             WHERE NOT (package_repo_owner_key(seg) = ANY(package_repo_structural_segments()))
        ), ARRAY[]::text[])
    END
    FROM segs;
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
                -- Owner parsed straight from the URL (package_repo_owner_from_url), not the stored
                -- repos.owner column — see that function's comment for why.
                SELECT b.id,
                       package_repo_owner_from_url(r.host, r.url) AS repo_owner,
                       -- Most ecosystems pass every maintainer role to matchOwnership() (NuGet
                       -- authors, Maven developers, ...); npm/upsertPackage.ts filters to
                       -- role='maintainer' only, so mirror that restriction here. RubyGems is
                       -- excluded entirely: runRubyGemsCoreLoop.ts re-syncs every gem daily and
                       -- always calls matchOwnership() with no maintainer evidence (ADR-0022), so
                       -- backfilling matched/unmatched here would just get reset to no_evidence
                       -- on the next sync. Maven is excluded too: upsertMaintainer falls back to
                       -- email/displayName in the username column when a person has no real
                       -- username (runMavenEnrichmentLoop.ts), and the DB has no way to tell that
                       -- fallback apart from a real <id> that happens to equal the email/display
                       -- name — value-equality heuristics misclassify real usernames, and a false
                       -- 'unmatched' costs more (-0.25) than a lost 'matched' would gain.
                       -- Email-shaped identities are rejected only when a non-whitespace char
                       -- appears on both sides of '@' (ownershipMatch.ts's /\S@\S/), matching
                       -- handles like '@vercel' still count.
                       package_repo_namespace_candidates(p.namespace)
                         || COALESCE(ARRAY(
                              SELECT m.username
                                FROM package_maintainers pm
                                JOIN maintainers m ON m.id = pm.maintainer_id
                               WHERE pm.package_id = cur.package_id
                                 AND p.ecosystem NOT IN ('rubygems', 'maven')
                                 AND (p.ecosystem <> 'npm' OR pm.role = 'maintainer')
                                 AND m.username !~ '\S@\S'
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
