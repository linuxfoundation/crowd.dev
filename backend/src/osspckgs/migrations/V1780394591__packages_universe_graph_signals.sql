-- Graph-derived signals for criticality scoring (ADR-0001 §Criticality scoring methodology).
-- Populated by the criticality worker; NULL until first pass.

ALTER TABLE packages_universe
  ADD COLUMN IF NOT EXISTS transitive_dependent_count bigint,
  ADD COLUMN IF NOT EXISTS centrality_score           numeric(10, 8);

-- Split dependent_packages_count into direct (MinimumDepth=1) vs transitive (MinimumDepth>1).

ALTER TABLE packages
  RENAME COLUMN dependent_packages_count TO dependent_count;

ALTER TABLE packages
  ADD COLUMN IF NOT EXISTS transitive_dependent_count bigint;

ALTER TABLE packages_universe
  RENAME COLUMN dependent_packages_count TO dependent_count;
