-- GitHub paths are case-insensitive, but repos.url is UNIQUE case-sensitively.
-- Writers that predate URL lowercasing (cargo initial sync on 2026-06-19, maven
-- enrichment before CM-1305) inserted mixed-case variants of rows that already
-- existed lowercase: ~40k duplicate groups, ~10k of them serving duplicate
-- security contacts from both variants. The CM-1305 backfills re-pointed
-- package links to the lowercase rows but left the stale variants behind.
--
-- Keeper per group: the all-lowercase row when present (links were already
-- re-pointed to it), else the lowest id. Only package_repos is primary data
-- and gets re-pointed (repo_docker comes along since it is a free UPDATE);
-- contacts, snapshots, and scorecard rows on losers are derived and re-fill
-- via the regular sweeps, so they are dropped with the loser rows.

CREATE TEMP TABLE repo_merge_members AS
SELECT id AS repo_id, keeper_id, id = keeper_id AS is_keeper
FROM (
    SELECT
        id,
        FIRST_VALUE(id) OVER (
            PARTITION BY LOWER(url)
            ORDER BY (url = LOWER(url)) DESC, id
        ) AS keeper_id,
        COUNT(*) OVER (PARTITION BY LOWER(url)) AS group_size
    FROM repos
    WHERE host = 'github'
) grouped
WHERE group_size > 1;

CREATE INDEX ON repo_merge_members (repo_id);
ANALYZE repo_merge_members;

-- Keep one link per (package, group), ranked the way consumers pick links
-- (sqlFragments bestRepoLink: confidence DESC, declared-on-ties) so the merge
-- preserves the strongest provenance; keeper status only breaks full ties.
-- ROW_NUMBER instead of an EXISTS check against the keeper because 3-variant
-- groups exist: two losers linking the same package would collide on
-- UNIQUE (package_id, repo_id) after the re-point below.
DELETE FROM package_repos
WHERE id IN (
    SELECT id
    FROM (
        SELECT pr.id,
               ROW_NUMBER() OVER (
                   PARTITION BY m.keeper_id, pr.package_id
                   ORDER BY pr.confidence DESC, (pr.source = 'declared') DESC,
                            m.is_keeper DESC, pr.verified_at DESC, pr.id
               ) AS rn
        FROM package_repos pr
        JOIN repo_merge_members m ON m.repo_id = pr.repo_id
    ) ranked
    WHERE rn > 1
);

UPDATE package_repos pr
SET repo_id = m.keeper_id
FROM repo_merge_members m
WHERE pr.repo_id = m.repo_id
  AND NOT m.is_keeper;

UPDATE repo_docker d
SET repo_id = m.keeper_id
FROM repo_merge_members m
WHERE d.repo_id = m.repo_id
  AND NOT m.is_keeper;

DELETE FROM repo_scorecard_checks c
USING repo_merge_members m
WHERE c.repo_id = m.repo_id
  AND NOT m.is_keeper;

-- packages.repository_url is denormalized and must keep matching the
-- canonical repos.url (the maven backfill updates the two atomically for the
-- same reason). Match against member urls while the loser rows still exist;
-- last_synced_at is the packages watermark, bumping it ships the correction
-- to Tinybird.
UPDATE packages p
SET repository_url = LOWER(p.repository_url), last_synced_at = NOW()
FROM repo_merge_members m
JOIN repos r ON r.id = m.repo_id
WHERE p.repository_url = r.url
  AND p.repository_url <> LOWER(p.repository_url);

-- Cascades security_contacts and repo_activity_snapshot on losers.
DELETE FROM repos r
USING repo_merge_members m
WHERE r.id = m.repo_id
  AND NOT m.is_keeper;

-- Groups that had no lowercase variant: lowercase the surviving row. Safe
-- against UNIQUE (url) — every row sharing LOWER(url) was in the same group,
-- and its losers are gone by now.
UPDATE repos r
SET url = LOWER(r.url), updated_at = NOW()
FROM repo_merge_members m
WHERE r.id = m.repo_id
  AND m.is_keeper
  AND r.url <> LOWER(r.url);

-- The recurrence-guard unique index lives in the next migration: it must be
-- built CONCURRENTLY (repos takes continuous worker writes), which cannot run
-- inside this transaction.
