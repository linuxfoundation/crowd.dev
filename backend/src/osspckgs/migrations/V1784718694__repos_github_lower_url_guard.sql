-- Recurrence guard for case-duplicate GitHub repos, split from the merge in
-- V1784718693: CONCURRENTLY cannot run inside a transaction, and under
-- flyway's mixed=true a shared file would demote the merge itself to
-- autocommit, losing its atomicity. The concurrent build keeps repos writable
-- for the workers during the scan, and doubles as verification of the merge —
-- a surviving duplicate fails the build.
--
-- GitHub-only: the only host with both confirmed duplicates and guaranteed
-- lowercase-on-write today (CASE_INSENSITIVE_HOSTS in canonicalizeRepoUrl
-- also covers gitlab.com, whose merge is a follow-up). On other hosts case
-- can be significant and writers do not normalize, so a wider index could
-- reject legitimately distinct repos.
--
-- A failed concurrent build leaves an INVALID index behind; the DROP lets a
-- re-run replace it, where IF NOT EXISTS would silently keep the broken one.
-- Also CONCURRENTLY: a plain drop takes an ACCESS EXCLUSIVE lock on repos,
-- stalling the workers on the retry path this exists for.
DROP INDEX CONCURRENTLY IF EXISTS repos_github_lower_url_uq;

CREATE UNIQUE INDEX CONCURRENTLY repos_github_lower_url_uq
    ON repos (LOWER(url))
    WHERE host = 'github';
