-- Ownership-evidence matching for package→repo links (CM-1394).
--
-- The per-package registry workers call matchOwnership() in TypeScript
-- (packages_worker/src/utils/ownershipMatch.ts). Cargo is the exception: its
-- pipeline is set-based over the crates.io dump and cannot call a parser per row,
-- so it needs the same normalization available in SQL. Both sides must stay in
-- step — the vanity suffix list and the prefix rule below mirror that module.

CREATE OR REPLACE FUNCTION package_repo_owner_key(p_identity text)
RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    WITH base AS (
        SELECT regexp_replace(lower(btrim(COALESCE(p_identity, ''))), '^@', '') AS v
    ),
    trimmed AS (
        SELECT v, substring(v from '-(?:ai|io|team|labs|oss|dev)$') AS suffix FROM base
    )
    SELECT NULLIF(
        regexp_replace(
            CASE
                WHEN suffix IS NOT NULL AND length(v) > length(suffix) + 1
                    THEN left(v, length(v) - length(suffix))
                ELSE v
            END,
            '[^a-z0-9]', '', 'g'
        ),
        ''
    )
    FROM trimmed;
$$;

CREATE OR REPLACE FUNCTION package_repo_owner_match(p_repo_owner text, p_candidates text[])
RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    owner_key text;
    keys      text[];
BEGIN
    owner_key := package_repo_owner_key(p_repo_owner);
    IF owner_key IS NULL THEN
        RETURN 'no_evidence';
    END IF;

    SELECT array_agg(k) INTO keys
      FROM (
        SELECT package_repo_owner_key(c) AS k
          FROM unnest(COALESCE(p_candidates, '{}'::text[])) AS c
      ) t
     WHERE k IS NOT NULL;

    IF keys IS NULL OR cardinality(keys) = 0 THEN
        RETURN 'no_evidence';
    END IF;

    -- Prefix equality on at least 4 characters: `tokio` matches `tokio-rs`, while
    -- short keys stay exact so `ab` cannot claim `abcdef`.
    IF EXISTS (
        SELECT 1
          FROM unnest(keys) AS k
         WHERE k = owner_key
            OR (length(k) <= length(owner_key) AND length(k) >= 4 AND owner_key LIKE k || '%')
            OR (length(owner_key) < length(k) AND length(owner_key) >= 4 AND k LIKE owner_key || '%')
    ) THEN
        RETURN 'matched';
    END IF;

    RETURN 'unmatched';
END;
$$;
