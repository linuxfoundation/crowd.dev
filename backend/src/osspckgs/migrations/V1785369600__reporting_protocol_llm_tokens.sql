-- Persist Bedrock LLM cost (USD) per parse so spend is measurable per file.
-- Null for deterministic rows (no LLM call); set for every parser='llm' row,
-- including degraded ones (a failed extraction still bills tokens).
ALTER TABLE security_policy_parses
    ADD COLUMN IF NOT EXISTS llm_cost_usd NUMERIC(12, 6);
