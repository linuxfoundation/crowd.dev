-- ============================================================
-- DOMAIN 1: UNIVERSE (Tier 3 → Tier 2 ranking input)
-- ============================================================
CREATE TABLE packages_universe (
    id bigserial PRIMARY KEY,
    purl text UNIQUE,
    ecosystem text NOT NULL,
    namespace text,
    name text NOT NULL,
    -- Cached latest 30-day window count. Written by the same weekly ranking worker that upserts rows into
    -- the downloads_last_30d table (keyed by purl/end_date). This column is the denormalized latest value
    -- used directly by rank_packages_universe() to avoid a join; the downloads_last_30d table holds the
    -- full rolling-window timeline.
    downloads_last_30d bigint,
    dependent_packages_count int,
    dependent_repos_count int,
    criticality_score numeric(10, 4),
    rank_in_ecosystem int,
    is_critical bool NOT NULL DEFAULT FALSE,
    -- Renamed from last_ranked_at (original pckgs.md spec) to last_rank_pass_at to make the
    -- every-pass update semantic explicit: updated unconditionally on each ranking run,
    -- not only when the rank changes. Mirrors the same column added to packages.
    last_rank_pass_at timestamptz
);

CREATE INDEX ON packages_universe (ecosystem, rank_in_ecosystem);

CREATE INDEX ON packages_universe (is_critical)
WHERE
    is_critical;

-- ============================================================
-- DOMAIN 2: TIER 2 PACKAGE DATA
-- ============================================================
CREATE TABLE packages (
    id bigserial PRIMARY KEY,
    purl text UNIQUE NOT NULL,
    ecosystem text NOT NULL,
    namespace text,
    name text NOT NULL,
    registry_url text,
    status text, -- 'active' | 'deprecated' | 'unpublished' | 'yanked'
    description text,
    homepage text,
    declared_repository_url text,
    repository_url text,
    licenses text[], -- SPDX normalized
    licenses_raw text,
    keywords text[],
    -- npm-specific (NULL for other ecosystems)
    dist_tags_latest text,
    dist_tags_next text,
    dist_tags_beta text,
    -- Aggregates (refreshed each ingestion run)
    versions_count int,
    latest_version text,
    first_release_at timestamptz,
    latest_release_at timestamptz,
    dependent_packages_count int,
    dependent_repos_count int,
    -- has_critical_vulnerability: TRUE iff latest_version is inside an active
    -- affected range of a critical advisory (CVSS >= 7.0) OR a MAL-* malicious-
    -- package advisory matches the package. Maintained by the deriveCriticalFlag
    -- activity in packages_worker/src/osv/. See ADR-0001 §`has_critical_vulnerability`
    -- semantics for the option-b + MAL- override rationale.
    has_critical_vulnerability bool NOT NULL DEFAULT FALSE,
    criticality_score numeric(10, 4),
    -- is_critical and last_rank_pass_at are not in the original pckgs.md spec; added so
    -- the packages table can answer "is this package critical?" without joining packages_universe,
    -- which is an ephemeral ranking workspace and gets truncated on every weekly pass.
    is_critical    bool NOT NULL DEFAULT FALSE,
    -- Set on every ranking pass (not just when rank changes) so queries can detect stale rows
    -- via last_rank_pass_at < NOW() - INTERVAL '8 days'.
    last_rank_pass_at timestamptz,
    ingestion_source text,
    last_synced_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX ON packages (ecosystem, COALESCE(namespace, ''), name);

CREATE INDEX ON packages (is_critical) WHERE is_critical;

CREATE INDEX ON packages (ecosystem, name);

CREATE INDEX ON packages USING gin (keywords);

-- Partial index on has_critical_vulnerability TRUE rows only — that's the bucket
-- the security overlay query needs ("list all packages with a known critical
-- vuln"). The FALSE rows dominate the table and don't need an index.
CREATE INDEX ON packages (has_critical_vulnerability)
WHERE
    has_critical_vulnerability;

CREATE INDEX ON packages (criticality_score DESC)
WHERE
    criticality_score IS NOT NULL;

CREATE TABLE package_name_history (
    id bigserial PRIMARY KEY,
    package_id bigint NOT NULL REFERENCES packages (id),
    old_name text NOT NULL,
    new_name text NOT NULL,
    changed_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX ON package_name_history (package_id);

CREATE TABLE package_funding_links (
    id bigserial PRIMARY KEY,
    package_id bigint NOT NULL REFERENCES packages (id),
    type TEXT, -- 'github' | 'patreon' | 'opencollective' | 'individual' | 'other'
    url text NOT NULL,
    UNIQUE (package_id, url)
);

-- ============================================================
-- VERSIONS — PARTITION BY HASH(package_id)
-- Hot query: WHERE package_id = X (all versions of a package).
-- 32 buckets → ~2.8M rows each at 90M total.
-- ============================================================
CREATE TABLE versions (
    id bigserial,
    package_id bigint NOT NULL REFERENCES packages (id),
    ecosystem text NOT NULL,
    number text NOT NULL,
    published_at timestamptz,
    -- Nullable: deps.dev PackageVersions does not expose is_latest; set by the npm/maven enricher
    -- workers that have authoritative latest-version data. NULL = unknown (not yet enriched).
    is_latest bool,
    -- Nullable for same reason: yanked status comes from registry-specific workers, not deps.dev.
    is_yanked bool,
    is_prerelease bool NOT NULL DEFAULT FALSE,
    license text, -- SPDX where available; can differ per version
    last_synced_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, package_id),
    UNIQUE (package_id, number)
)
PARTITION BY HASH (package_id);

CREATE TABLE versions_p0 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 0);

CREATE TABLE versions_p1 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 1);

CREATE TABLE versions_p2 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 2);

CREATE TABLE versions_p3 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 3);

CREATE TABLE versions_p4 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 4);

CREATE TABLE versions_p5 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 5);

CREATE TABLE versions_p6 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 6);

CREATE TABLE versions_p7 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 7);

CREATE TABLE versions_p8 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 8);

CREATE TABLE versions_p9 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 9);

CREATE TABLE versions_p10 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 10);

CREATE TABLE versions_p11 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 11);

CREATE TABLE versions_p12 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 12);

CREATE TABLE versions_p13 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 13);

CREATE TABLE versions_p14 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 14);

CREATE TABLE versions_p15 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 15);

CREATE TABLE versions_p16 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 16);

CREATE TABLE versions_p17 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 17);

CREATE TABLE versions_p18 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 18);

CREATE TABLE versions_p19 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 19);

CREATE TABLE versions_p20 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 20);

CREATE TABLE versions_p21 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 21);

CREATE TABLE versions_p22 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 22);

CREATE TABLE versions_p23 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 23);

CREATE TABLE versions_p24 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 24);

CREATE TABLE versions_p25 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 25);

CREATE TABLE versions_p26 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 26);

CREATE TABLE versions_p27 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 27);

CREATE TABLE versions_p28 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 28);

CREATE TABLE versions_p29 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 29);

CREATE TABLE versions_p30 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 30);

CREATE TABLE versions_p31 PARTITION OF versions
FOR VALUES WITH (MODULUS 32, REMAINDER 31);

CREATE INDEX ON versions (published_at DESC);

CREATE INDEX ON versions (package_id)
WHERE
    is_latest;

-- ============================================================
-- PACKAGE DEPENDENCIES — PARTITION BY HASH(depends_on_id)
--
-- Terminology:
--   downstream = packages that depend ON a given package (its consumers)
--   upstream   = packages that a given package depends ON (its suppliers)
--
-- Security alerting hot query: "vulnerability in X — who is at risk?"
--   → WHERE depends_on_id = X  (find all downstream consumers of X)
--   → lands in one partition → fast
-- 64 buckets → ~18M rows each at 1.15B total.
--
-- Upstream query: "what does version X depend on?"
--   → WHERE version_id = X  (scatters across all 64 partitions)
--   → slower by design; use the index on (version_id, depends_on_id, dependency_kind)
--
-- package_id: no standalone FK; satisfies composite FK to
--   partitioned versions(id, package_id).
-- depends_on_id: FK to packages; also second component of
--   composite FK for resolved depends_on_version_id.
-- ============================================================
CREATE TABLE package_dependencies (
    id bigserial,
    package_id bigint NOT NULL,
    version_id bigint NOT NULL,
    depends_on_id bigint NOT NULL REFERENCES packages (id),
    depends_on_version_id bigint, -- resolved version; NULL if unknown
    version_constraint text, -- declared constraint e.g. '^1.2.3'
    dependency_kind text NOT NULL, -- 'direct' | 'dev' | 'peer'
    is_optional bool NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id, depends_on_id),
    UNIQUE (version_id, depends_on_id, dependency_kind),
    FOREIGN KEY (version_id, package_id) REFERENCES versions (id, package_id),
    FOREIGN KEY (depends_on_version_id, depends_on_id) REFERENCES versions (id, package_id)
)
PARTITION BY HASH (depends_on_id);

CREATE TABLE package_dependencies_p0 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 0);

CREATE TABLE package_dependencies_p1 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 1);

CREATE TABLE package_dependencies_p2 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 2);

CREATE TABLE package_dependencies_p3 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 3);

CREATE TABLE package_dependencies_p4 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 4);

CREATE TABLE package_dependencies_p5 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 5);

CREATE TABLE package_dependencies_p6 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 6);

CREATE TABLE package_dependencies_p7 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 7);

CREATE TABLE package_dependencies_p8 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 8);

CREATE TABLE package_dependencies_p9 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 9);

CREATE TABLE package_dependencies_p10 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 10);

CREATE TABLE package_dependencies_p11 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 11);

CREATE TABLE package_dependencies_p12 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 12);

CREATE TABLE package_dependencies_p13 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 13);

CREATE TABLE package_dependencies_p14 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 14);

CREATE TABLE package_dependencies_p15 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 15);

CREATE TABLE package_dependencies_p16 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 16);

CREATE TABLE package_dependencies_p17 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 17);

CREATE TABLE package_dependencies_p18 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 18);

CREATE TABLE package_dependencies_p19 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 19);

CREATE TABLE package_dependencies_p20 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 20);

CREATE TABLE package_dependencies_p21 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 21);

CREATE TABLE package_dependencies_p22 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 22);

CREATE TABLE package_dependencies_p23 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 23);

CREATE TABLE package_dependencies_p24 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 24);

CREATE TABLE package_dependencies_p25 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 25);

CREATE TABLE package_dependencies_p26 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 26);

CREATE TABLE package_dependencies_p27 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 27);

CREATE TABLE package_dependencies_p28 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 28);

CREATE TABLE package_dependencies_p29 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 29);

CREATE TABLE package_dependencies_p30 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 30);

CREATE TABLE package_dependencies_p31 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 31);

CREATE TABLE package_dependencies_p32 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 32);

CREATE TABLE package_dependencies_p33 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 33);

CREATE TABLE package_dependencies_p34 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 34);

CREATE TABLE package_dependencies_p35 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 35);

CREATE TABLE package_dependencies_p36 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 36);

CREATE TABLE package_dependencies_p37 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 37);

CREATE TABLE package_dependencies_p38 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 38);

CREATE TABLE package_dependencies_p39 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 39);

CREATE TABLE package_dependencies_p40 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 40);

CREATE TABLE package_dependencies_p41 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 41);

CREATE TABLE package_dependencies_p42 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 42);

CREATE TABLE package_dependencies_p43 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 43);

CREATE TABLE package_dependencies_p44 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 44);

CREATE TABLE package_dependencies_p45 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 45);

CREATE TABLE package_dependencies_p46 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 46);

CREATE TABLE package_dependencies_p47 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 47);

CREATE TABLE package_dependencies_p48 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 48);

CREATE TABLE package_dependencies_p49 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 49);

CREATE TABLE package_dependencies_p50 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 50);

CREATE TABLE package_dependencies_p51 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 51);

CREATE TABLE package_dependencies_p52 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 52);

CREATE TABLE package_dependencies_p53 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 53);

CREATE TABLE package_dependencies_p54 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 54);

CREATE TABLE package_dependencies_p55 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 55);

CREATE TABLE package_dependencies_p56 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 56);

CREATE TABLE package_dependencies_p57 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 57);

CREATE TABLE package_dependencies_p58 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 58);

CREATE TABLE package_dependencies_p59 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 59);

CREATE TABLE package_dependencies_p60 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 60);

CREATE TABLE package_dependencies_p61 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 61);

CREATE TABLE package_dependencies_p62 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 62);

CREATE TABLE package_dependencies_p63 PARTITION OF package_dependencies
FOR VALUES WITH (MODULUS 64, REMAINDER 63);

-- upstream: version-specific lookup within the single partition
CREATE INDEX ON package_dependencies (depends_on_id, depends_on_version_id);

-- downstream: version_id queries scatter across 64 partitions but use this index
CREATE INDEX ON package_dependencies (version_id);

-- ============================================================
-- DOMAIN 3: REPOSITORIES
-- ============================================================
CREATE TABLE repos (
    id bigserial PRIMARY KEY,
    url text UNIQUE NOT NULL,
    host text, -- 'github' | 'gitlab' | 'bitbucket' | 'other'
    owner TEXT,
    name text,
    description text,
    primary_language text,
    topics text[],
    stars int,
    forks int,
    watchers int,
    open_issues int,
    last_commit_at timestamptz,
    -- Nullable: deps.dev ProjectsLatest does not expose archived/disabled/is_fork.
    -- These are populated by the GitHub API enricher worker. NULL = not yet enriched.
    archived bool,
    disabled bool,
    is_fork bool,
    -- DEFAULT NOW() added: fallback when upstream source does not provide a creation timestamp.
    created_at timestamptz DEFAULT NOW(),
    homepage text,
    -- raw_project_type/raw_project_name preserve deps.dev's original project identity (e.g. "GITLAB",
    -- "github.com/owner/repo") so self-hosted GitLab instances can be detected later without backfill.
    -- canonicalRepoUrl() uses these to build the canonical url; they remain queryable for debugging.
    raw_project_type text,
    raw_project_name text,
    -- Scorecard aggregate; per-check detail in repo_scorecard_checks
    scorecard_score numeric(3, 1),
    scorecard_last_run_at timestamptz,
    -- Last time dockerhub-sync probed this repo for a published Docker image (Dockerfile
    -- detection + Hub candidate lookup). NULL = never checked. Separate from last_synced_at
    -- because discovery cadence (weeks) differs from light-metadata refresh cadence (daily).
    docker_checked_at timestamptz,
    -- Nullable with no default: multiple enrichers (deps.dev, GitHub worker, Scorecard) each write
    -- different columns at different times. NOT NULL DEFAULT would stamp a "synced" timestamp on
    -- first insert even when most columns are still NULL, making freshness checks misleading.
    last_synced_at timestamptz
);

CREATE INDEX ON repos (host, OWNER, name);

CREATE INDEX ON repos (stars DESC);

CREATE INDEX ON repos (scorecard_score)
WHERE
    scorecard_score IS NOT NULL;

-- Partial index for the dockerhub-sync discovery backlog query: pages repos that have
-- never been probed for a Docker image. Once docker_checked_at is set the row drops out
-- of the index, so this stays small even as the repos table grows.
CREATE INDEX repos_docker_pending_idx ON repos (id)
WHERE
    host = 'github' AND docker_checked_at IS NULL;

-- OpenSSF Scorecard per-check detail (~18 named checks)
CREATE TABLE repo_scorecard_checks (
    id bigserial PRIMARY KEY,
    repo_id bigint NOT NULL REFERENCES repos (id),
    check_name text NOT NULL, -- 'Binary-Artifacts' | 'Branch-Protection' | ...
    score numeric(3, 1),
    reason text,
    UNIQUE (repo_id, check_name)
);

-- Docker images published by a repo (one-to-many)
CREATE TABLE repo_docker (
    id bigserial PRIMARY KEY,
    repo_id bigint REFERENCES repos (id), -- nullable: image may predate repo link
    image_name text NOT NULL,
    pulls bigint,
    stars int,
    last_synced_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX ON repo_docker (image_name);

CREATE INDEX ON repo_docker (repo_id)
WHERE
    repo_id IS NOT NULL;

-- Supports the dockerhub-sync refresh query (WHERE last_synced_at < NOW() - interval).
CREATE INDEX repo_docker_stale_idx ON repo_docker (last_synced_at);

-- ============================================================
-- REPO DOCKER PULLS DAILY
-- One row per image per day storing the *lifetime* pull_count as returned
-- by hub.docker.com/v2/repositories/<image>. Docker Hub does not expose
-- per-day download counts, so daily deltas are derived at query time:
--   pulls_total - LAG(pulls_total) OVER (PARTITION BY image_name ORDER BY date)
-- Keyed by image_name (matches repo_docker UNIQUE) so rows survive a
-- repo_docker re-discovery without an FK cascade.
--
-- Partitioned monthly via pg_partman — same setup as downloads_daily; add a
-- partman.create_parent('public.repo_docker_pulls_daily', 'date', '1 month', 3)
-- call alongside the downloads_daily registration.
-- ============================================================
CREATE TABLE repo_docker_pulls_daily (
    image_name text NOT NULL,
    date date NOT NULL,
    pulls_total bigint NOT NULL,
    PRIMARY KEY (image_name, date)
)
PARTITION BY RANGE (date);

-- Package → repo provenance (monorepos publish N packages from one repo)
CREATE TABLE package_repos (
    id bigserial PRIMARY KEY,
    package_id bigint NOT NULL REFERENCES packages (id),
    repo_id bigint NOT NULL REFERENCES repos (id),
    -- source values TBD pending alignment on data provider (e.g. deps.dev)
    source text NOT NULL, -- 'declared' | 'deps_dev' | 'heuristic' | 'manual'
    confidence numeric(3, 2) NOT NULL CHECK (confidence BETWEEN 0.00 AND 1.00),
    verified_at timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (package_id, repo_id)
);

CREATE INDEX ON package_repos (repo_id);

-- ============================================================
-- DOMAIN 4: SECURITY (OSV-shaped)
-- One advisory → many affected packages → many version ranges.
-- Mirrors OSV schema: a single advisory can affect N packages
-- across different ecosystems (e.g. a vuln in a shared C lib).
-- ============================================================
CREATE TABLE advisories (
    id bigserial PRIMARY KEY,
    osv_id text UNIQUE NOT NULL,   -- SourceID from deps.dev BQ (GHSA-xxx, CVE-xxx, OSV-xxx, etc.)
    source text,                   -- 'GHSA' | 'OSV' | 'NVD' | 'NSWG' etc. (BQ: Source)
    source_url text,               -- upstream advisory URL (BQ: SourceURL)
    aliases text[], -- CVE-XXXX, GHSA-...
    severity text, -- 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL'
    cvss numeric(3, 1),
    -- Provenance of the cvss value above. Lets downstream consumers distinguish
    -- a real vendor-supplied vector from a synthesized qualitative fallback.
    -- See ADR-0001 §CVSS scoring strategy. Allowed values:
    --   'osv_cvss_v3'              numeric score from a CVSS_V3 vector
    --   'osv_cvss_v4'              reserved; v4 numeric scoring deferred
    --   'osv_qualitative_fallback' synthesized from database_specific.severity
    --   'osv_malicious_package'    MAL-* id with no CVSS vector
    -- Extensible to 'ghsa' | 'nvd' as additional sources come online.
    cvss_source text,
    -- >= 7.0 intentional: treat HIGH + CRITICAL both as actionable
    is_critical bool GENERATED ALWAYS AS (cvss >= 7.0) STORED,
    summary text,
    details text,
    published_at timestamptz,
    modified_at timestamptz        -- NULL for BQ-sourced rows; tracked in-house on re-sync
);

-- osv_id index omitted: UNIQUE constraint above already creates one.
CREATE INDEX ON advisories (is_critical)
WHERE
    is_critical;

-- Advisory → package mapping. One advisory can affect many packages.
-- package_id is NULL when the package exists in OSV but not yet in our DB.
CREATE TABLE advisory_packages (
    id bigserial PRIMARY KEY,
    advisory_id bigint NOT NULL REFERENCES advisories (id),
    package_id bigint REFERENCES packages (id),
    ecosystem text NOT NULL,
    package_name text NOT NULL,
    UNIQUE (advisory_id, ecosystem, package_name)
);

CREATE INDEX ON advisory_packages (ecosystem, package_name);

CREATE INDEX ON advisory_packages (package_id)
WHERE
    package_id IS NOT NULL;

-- Drives the resolveMissingPackageIds catch-up UPDATE in deriveCriticalFlag:
-- the query filters WHERE package_id IS NULL and joins on (ecosystem,
-- package_name). The non-partial (ecosystem, package_name) index above is
-- usable here too (the planner just adds a Filter on package_id IS NULL), but
-- as the table grows the vast majority of rows have package_id IS NOT NULL,
-- so the non-partial scan ends up filtering out most of what it reads. This
-- partial index only contains the still-unresolved rows, keeping it tiny
-- regardless of total table size and making the daily catch-up O(unresolved)
-- instead of O(total).
CREATE INDEX ON advisory_packages (ecosystem, package_name)
WHERE
    package_id IS NULL;

-- Version ranges affected by an advisory per package. Populated by the OSV
-- ingest worker (packages_worker/src/osv) using introduced_version /
-- fixed_version / last_affected. range_raw / unaffected_raw are reserved
-- for the deps.dev BQ ingest worker (future): that worker writes the raw
-- range strings without parsing into structured boundaries. The OSV upsert
-- path only deletes rows where range_raw / unaffected_raw are both NULL,
-- so deps.dev rows are not clobbered when OSV re-syncs.
-- COALESCE prevents silent duplicates when introduced_version is NULL.
CREATE TABLE advisory_affected_ranges (
    id bigserial PRIMARY KEY,
    advisory_package_id bigint NOT NULL REFERENCES advisory_packages (id),
    introduced_version text, -- NULL = unknown start
    fixed_version text,      -- NULL = no fix yet
    last_affected text,      -- NULL = no known upper bound
    range_raw text,          -- raw AffectedVersions string from deps.dev BQ
    unaffected_raw text      -- raw UnaffectedVersions string from deps.dev BQ
);

-- Full-tuple uniqueness so two ranges sharing introduced_version but differing
-- in fixed_version or last_affected (cross-distro patches, partial fixes in a
-- single advisory) both survive insertion. The narrower (advisory_package_id,
-- introduced_version) form silently collapsed those cases to one row, dropping
-- the wider range and under-reporting vulnerable windows in the derive step.
-- See ADR-0001 §`advisory_affected_ranges` uniqueness scope.
CREATE UNIQUE INDEX ON advisory_affected_ranges (
    advisory_package_id,
    COALESCE(introduced_version, ''),
    COALESCE(fixed_version, ''),
    COALESCE(last_affected, '')
);

CREATE INDEX ON advisory_affected_ranges (advisory_package_id);

-- ============================================================
-- MAINTAINERS
-- ============================================================
CREATE TABLE maintainers (
    id bigserial PRIMARY KEY,
    ecosystem text NOT NULL,
    username text NOT NULL,
    display_name text,
    url text,
    email_hash text, -- SHA-256; never raw email (GDPR)
    github_login text,
    UNIQUE (ecosystem, username)
);

CREATE INDEX ON maintainers (github_login)
WHERE
    github_login IS NOT NULL;

CREATE TABLE package_maintainers (
    id bigserial PRIMARY KEY,
    package_id bigint NOT NULL REFERENCES packages (id),
    maintainer_id bigint NOT NULL REFERENCES maintainers (id),
    role TEXT, -- 'author' | 'maintainer'
    UNIQUE (package_id, maintainer_id)
);

-- ============================================================
-- DOWNLOADS
--
-- Two tables track download volume at different tiers and granularities:
--
--   downloads_daily   (tier 2 — packages)
--     Source of truth for daily download counts. One row per package per day.
--     No denormalized rollup on the packages table — consumers SUM over this
--     table when they need a window (e.g. last 30 days).
--
--   downloads_last_30d (tier 3 — packages_universe)
--     Rolling 30-day download timeline keyed by purl. Each row represents one
--     30-day window (start_date..end_date). Keyed by purl so rows survive the
--     weekly truncation of packages_universe. The latest window's count is also
--     cached in packages_universe.downloads_last_30d for fast access by the
--     criticality-ranking function (no join needed).
--
-- ============================================================
-- DOWNLOADS DAILY (tier 2 — packages, daily granularity)
--
-- Partitioned by month via pg_partman. pg_partman MUST be enabled in OCI
-- config before this migration runs:
--   OCI Console → Database → Configuration → Extensions → enable pg_partman
--
-- After enabling, run the setup below (once, outside Flyway or in a
-- separate migration) to register pg_partman and create initial partitions:
--
--   CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;
--
--   SELECT partman.create_parent(
--       p_parent_table => 'public.downloads_daily',
--       p_control      => 'date',
--       p_interval     => '1 month',
--       p_premake      => 3   -- pre-creates 3 future monthly partitions
--   );
--
--   -- pg_cron job to maintain partitions (also needs pg_cron enabled in OCI):
--   SELECT cron.schedule('partman-maintain', '0 1 * * *',
--       $$CALL partman.run_maintenance_proc()$$);
--
-- Without this setup, inserts into downloads_daily will fail with
-- "no partition found for row". The table structure below is correct;
-- only the partition management setup is deferred.
--
-- PK includes date because Postgres requires the partition key to be
-- part of the primary key on range-partitioned tables.
-- ============================================================
CREATE TABLE downloads_daily (
    id bigserial,
    package_id bigint NOT NULL REFERENCES packages (id),
    date date NOT NULL,
    count bigint NOT NULL,
    PRIMARY KEY (id, date),
    UNIQUE (package_id, date)
)
PARTITION BY RANGE (date);

-- ============================================================
-- DOWNLOADS LAST 30D (tier 3 — packages_universe, rolling 30-day granularity)
--
-- Historical timeline of rolling 30-day download counts, keyed by purl.
-- Each row captures one window: downloads from start_date to end_date (inclusive).
-- Keyed by purl (not packages_universe.id) so rows survive the weekly
-- truncation of packages_universe. The latest window is also written
-- to packages_universe.downloads_last_30d column for fast access by the ranking function.
--
-- Writers should upsert: INSERT ... ON CONFLICT (purl, end_date) DO UPDATE SET count = EXCLUDED.count, start_date = EXCLUDED.start_date
-- PK includes end_date because Postgres requires the partition key to be
-- part of the primary key on range-partitioned tables.
--
-- Partitioned by month via pg_partman. pg_partman MUST be enabled in OCI
-- config before this migration runs:
--   OCI Console → Database → Configuration → Extensions → enable pg_partman
--
-- After enabling, run the setup below (once, outside Flyway or in a
-- separate migration) to register pg_partman and create initial partitions:
--
--   CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;
--
--   SELECT partman.create_parent(
--       p_parent_table => 'public.downloads_last_30d',
--       p_control      => 'end_date',
--       p_interval     => '1 month',
--       p_premake      => 3   -- pre-creates 3 future monthly partitions
--   );
--
--   -- pg_cron job to maintain partitions (also needs pg_cron enabled in OCI):
--   SELECT cron.schedule('partman-maintain-30d', '0 2 * * *',
--       $$CALL partman.run_maintenance_proc()$$);
--
-- Without this setup, inserts into downloads_last_30d will fail with
-- "no partition found for row". The table structure below is correct;
-- only the partition management setup is deferred.
--
-- ============================================================
CREATE TABLE downloads_last_30d (
    id bigserial,
    purl text NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL,
    count bigint NOT NULL,
    PRIMARY KEY (id, end_date),
    UNIQUE (purl, end_date)
)
PARTITION BY RANGE (end_date);

CREATE INDEX ON downloads_last_30d (purl, end_date DESC);

-- ============================================================
-- CRITICALITY RANKING FUNCTION
-- ============================================================
CREATE OR REPLACE FUNCTION rank_packages_universe(
    weight_downloads            numeric,
    weight_dependent_repos      numeric,
    weight_dependent_packages   numeric,
    log_smoothing               numeric,
    critical_top_n_by_ecosystem jsonb
)
RETURNS TABLE (scored_rows int, ranked_rows int, propagated_rows int)
LANGUAGE plpgsql
AS $$
DECLARE
    n_scored     int;
    n_ranked     int;
    n_propagated int;
BEGIN
    -- Step 1: recompute scores; only touch rows whose score changed.
    -- log_smoothing is added before LN() to avoid LN(0) on zero-count rows
    -- and to compress the gap between small and large values (e.g. LN(1)=0
    -- vs LN(2)≈0.69 gives a gentler floor than LN(0)=-∞). Typically 1.0.
    --
    -- Until the npm-registry / Maven downloads enricher runs, downloads_last_30d
    -- is NULL on every row. weight_downloads contributes 0 to the score;
    -- ranking effectively reduces to:
    --   LN(1 + dependent_repos_count)     * weight_dependent_repos
    -- + LN(1 + dependent_packages_count)  * weight_dependent_packages
    UPDATE packages_universe SET last_rank_pass_at = NOW();

    WITH new_scores AS (
        SELECT
            id,
            ( LN(log_smoothing + COALESCE(downloads_last_30d, 0))            * weight_downloads
            + LN(log_smoothing + COALESCE(dependent_repos_count, 0))    * weight_dependent_repos
            + LN(log_smoothing + COALESCE(dependent_packages_count, 0)) * weight_dependent_packages
            )::numeric(10, 4) AS new_score
        FROM packages_universe
    )
    UPDATE packages_universe pu
    SET    criticality_score = ns.new_score
    FROM   new_scores ns
    WHERE  pu.id = ns.id
      AND  pu.criticality_score IS DISTINCT FROM ns.new_score;

    GET DIAGNOSTICS n_scored = ROW_COUNT;

    -- Step 2: rank within ecosystem; flag is_critical via JSONB lookup.
    -- Only purl-having rows are ranked (null purls can't propagate to packages).
    -- Tie-break by id keeps ranks deterministic across runs so IS DISTINCT FROM
    -- doesn't no-op-write equal-score rows on every call.
    WITH ranked AS (
        SELECT
            id,
            ecosystem,
            ROW_NUMBER() OVER (
                PARTITION BY ecosystem
                ORDER BY criticality_score DESC NULLS LAST, id
            ) AS r
        FROM packages_universe
        WHERE purl IS NOT NULL
    ),
    with_flag AS (
        SELECT
            id,
            r,
            COALESCE(
                r <= (critical_top_n_by_ecosystem ->> ecosystem)::int,
                FALSE
            ) AS new_is_critical
        FROM ranked
    )
    UPDATE packages_universe pu
    SET    rank_in_ecosystem = wf.r,
           is_critical       = wf.new_is_critical
    FROM   with_flag wf
    WHERE  pu.id = wf.id
      AND  ( pu.rank_in_ecosystem IS DISTINCT FROM wf.r
          OR pu.is_critical       IS DISTINCT FROM wf.new_is_critical );

    GET DIAGNOSTICS n_ranked = ROW_COUNT;

    -- Step 3: propagate criticality_score onto Tier-2 packages rows.
    UPDATE packages p
    SET    criticality_score = pu.criticality_score
    FROM   packages_universe pu
    WHERE  p.purl = pu.purl
      AND  p.criticality_score IS DISTINCT FROM pu.criticality_score;

    GET DIAGNOSTICS n_propagated = ROW_COUNT;

    RETURN QUERY SELECT n_scored, n_ranked, n_propagated;
END;
$$;
