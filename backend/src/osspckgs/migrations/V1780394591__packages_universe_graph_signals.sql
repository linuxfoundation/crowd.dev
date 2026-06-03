-- Graph-derived signals for criticality scoring (ADR-0001 §Criticality scoring methodology).
-- Populated by the criticality worker; NULL until first pass.

ALTER TABLE packages_universe
  ADD COLUMN IF NOT EXISTS transitive_dependent_count bigint,
  ADD COLUMN IF NOT EXISTS centrality_score           numeric(10, 8);
