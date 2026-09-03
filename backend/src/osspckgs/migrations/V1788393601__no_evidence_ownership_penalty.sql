-- Enable the no_evidence ownership penalty (CM-1394).
--
-- V1788307200 added the ownership_match column and scoring function but deferred
-- the no_evidence branch pending CM-1394 writing real values. All writers now set
-- the column, so the −0.10 penalty is activated here.
CREATE OR REPLACE FUNCTION package_repo_confidence(
    p_source           text,
    p_ecosystem        text,
    p_signal           text,
    p_ownership_match  text,
    p_provenance       text,
    p_archived         bool,
    p_is_fork          bool,
    p_disabled         bool,
    p_host             text,
    p_competing_github bool,
    p_repo_id          bigint
)
RETURNS numeric(12, 9)
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    base            numeric;
    source_priority int;
    offset_units    bigint;
BEGIN
    base := CASE p_source
        WHEN 'manual'    THEN 0.99
        WHEN 'heuristic' THEN 0.30
        WHEN 'deps_dev'  THEN CASE p_provenance
            WHEN 'SLSA_ATTESTATION'             THEN 0.99
            WHEN 'RUBYGEMS_PUBLISH_ATTESTATION' THEN 0.95
            WHEN 'PYPI_PUBLISH_ATTESTATION'     THEN 0.95
            WHEN 'GO_ORIGIN'                    THEN 0.90
            ELSE 0.50
        END
        -- maven splits off npm/cargo/the rest: POM <scm> blocks are notoriously
        -- stale (legacy SVN URLs, org renames, dead mirrors).
        WHEN 'declared' THEN CASE WHEN p_ecosystem = 'maven' THEN 0.80 ELSE 0.85 END
        ELSE 0.30
    END;

    -- Signal and ownership adjust the declared tier only. A deps.dev publish
    -- attestation already proves the publisher–repo relationship, and manual links
    -- are operator-pinned.
    IF p_source = 'declared' THEN
        IF p_signal = 'secondary' THEN
            base := base - 0.10;
        END IF;

        IF p_ownership_match = 'unmatched' THEN
            base := base - 0.25;
        ELSIF p_ownership_match = 'no_evidence' THEN
            base := base - 0.10;
        END IF;
    END IF;

    -- Disabled overrides all state penalties but still gets the uniqueness offset so
    -- the no-ties invariant holds and a stronger claim can displace the stored row.
    -- Proportional scaling preserves pre-disabled claim ordering across sources.
    IF p_disabled IS TRUE THEN
        base := 0.05 + LEAST(base, 0.99) * 0.004;
    ELSE
        IF p_archived IS TRUE THEN
            base := base - 0.20;
        END IF;

        IF p_is_fork IS TRUE THEN
            base := base - 0.10;
        END IF;

        IF p_competing_github IS TRUE AND COALESCE(p_host, '') <> 'github' THEN
            base := base - 0.05;
        END IF;
    END IF;

    base := GREATEST(base, 0.05);

    source_priority := CASE p_source
        WHEN 'manual'    THEN 3
        WHEN 'deps_dev'  THEN 2
        WHEN 'declared'  THEN 1
        ELSE 0
    END;

    -- Two repos on the same package collide only if their ids are congruent mod 1e6
    -- and their sources share a priority band.
    offset_units := source_priority::bigint * 1000000 + COALESCE(p_repo_id, 0) % 1000000;

    RETURN LEAST(base + offset_units * 0.000000001, 0.999999999);
END;
$$;
