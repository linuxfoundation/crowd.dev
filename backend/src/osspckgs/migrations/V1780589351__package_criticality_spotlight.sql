-- Manual override table for criticality scoring (ADR-0001 §Spotlight overrides).
-- Packages listed here are forced is_critical = TRUE regardless of computed score.
-- Applied after ranking inside rank_packages_universe() so overrides survive
-- every automated re-rank pass.
--
-- rationale, added_by, added_at are required — the table must stay auditable.
-- namespace is nullable: cargo crates have no namespace, Maven artifacts do.
-- The UNIQUE key uses COALESCE so (ecosystem, NULL namespace, name) is enforced correctly.

CREATE TABLE package_criticality_spotlight (
    id          bigserial   PRIMARY KEY,
    ecosystem   text        NOT NULL,
    namespace   text,
    name        text        NOT NULL,
    rationale   text        NOT NULL,
    added_by    text        NOT NULL,
    added_at    timestamptz NOT NULL DEFAULT NOW()
);

-- Functional unique index: COALESCE treats NULL namespace as '' so that
-- (cargo, NULL, tokio) and (cargo, NULL, serde) are unique but a duplicate
-- (cargo, NULL, tokio) entry is rejected.
CREATE UNIQUE INDEX ON package_criticality_spotlight
    (ecosystem, COALESCE(namespace, ''), name);
