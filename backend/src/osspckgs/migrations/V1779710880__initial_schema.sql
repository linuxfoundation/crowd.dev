-- ============================================================
-- DOMAIN 1: UNIVERSE (Tier 3 → Tier 2 ranking input)
-- ============================================================
CREATE TABLE packages_universe (
    id bigserial PRIMARY KEY,
    purl text UNIQUE,
    ecosystem text NOT NULL,
    namespace text,
    name text NOT NULL,
    downloads_30d bigint,
    dependent_packages_count int,
    dependent_repos_count int,
    criticality_score numeric(10, 4),
    rank_in_ecosystem int,
    is_critical bool NOT NULL DEFAULT FALSE,
    last_ranked_at timestamptz
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
    downloads_last_month bigint,
    -- TODO: define semantics before enabling. Options:
    --   a) fixed_version IS NULL on any advisory range → "no fix released yet" (simple, opt 2)
    --   b) latest_version falls inside an affected range → "currently vulnerable" (correct, needs semver comparison per ecosystem)
    -- has_critical_vulnerability bool NOT NULL DEFAULT FALSE,
    criticality_score numeric(10, 4),
    ingestion_source text,
    last_synced_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX ON packages (ecosystem, COALESCE(namespace, ''), name);

CREATE INDEX ON packages (ecosystem, name);

CREATE INDEX ON packages USING gin (keywords);

CREATE INDEX ON packages (downloads_last_month DESC)
WHERE
    status = 'active';

-- INDEX on has_critical_vulnerability removed — column is commented out above.
-- Uncomment both when semantics are decided.

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
    is_latest bool NOT NULL DEFAULT FALSE,
    is_yanked bool NOT NULL DEFAULT FALSE,
    is_prerelease bool NOT NULL DEFAULT FALSE,
    license text, -- SPDX where available; can differ per version
    download_count bigint, -- per-version where available (npm, crates)
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
    archived bool NOT NULL DEFAULT FALSE,
    disabled bool NOT NULL DEFAULT FALSE,
    is_fork bool NOT NULL DEFAULT FALSE,
    created_at timestamptz,
    -- Scorecard aggregate; per-check detail in repo_scorecard_checks
    scorecard_score numeric(3, 1),
    scorecard_last_run_at timestamptz,
    last_synced_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX ON repos (host, OWNER, name);

CREATE INDEX ON repos (stars DESC);

CREATE INDEX ON repos (scorecard_score)
WHERE
    scorecard_score IS NOT NULL;

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
    osv_id text UNIQUE NOT NULL,
    aliases text[], -- CVE-XXXX, GHSA-...
    severity text, -- 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL'
    cvss numeric(3, 1),
    -- >= 7.0 intentional: treat HIGH + CRITICAL both as actionable
    is_critical bool GENERATED ALWAYS AS (cvss >= 7.0) STORED,
    summary text,
    details text,
    published_at timestamptz,
    modified_at timestamptz
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

-- Version ranges affected by an advisory per package.
-- COALESCE prevents silent duplicates when introduced_version is NULL.
CREATE TABLE advisory_affected_ranges (
    id bigserial PRIMARY KEY,
    advisory_package_id bigint NOT NULL REFERENCES advisory_packages (id),
    introduced_version text, -- NULL = unknown start
    fixed_version text, -- NULL = no fix yet
    last_affected text -- NULL = no known upper bound
);

CREATE UNIQUE INDEX ON advisory_affected_ranges (advisory_package_id, COALESCE(introduced_version, ''));

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
-- DOWNLOADS (time-series, partitioned by month via pg_partman)
--
-- pg_partman MUST be enabled in OCI config before this migration runs:
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
    package_id bigint NOT NULL,
    date date NOT NULL,
    count bigint NOT NULL,
    PRIMARY KEY (id, date),
    UNIQUE (package_id, date)
)
PARTITION BY RANGE (date);
