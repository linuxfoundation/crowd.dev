-- Prod validation for the reporting-protocol pipeline (all read-only).
-- Run section by section; each header says what healthy looks like.

-- 1. Parse progress and split
-- Healthy: rows climb toward ~6.1k distinct security blobs; deterministic ≈ 65-75%,
-- degraded well under 10%.
SELECT source_kind, parser, status, COUNT(*)
FROM security_policy_parses
GROUP BY source_kind, parser, status
ORDER BY source_kind, parser, status;

-- 2. Assembly coverage
-- Healthy: repos_with_protocol approaches the critical population; declared_pct ≈ 15-20%
-- (files + PVR flag overlap).
WITH critical AS (
    SELECT DISTINCT r.id
    FROM repos r
    JOIN package_repos pr ON pr.repo_id = r.id
    JOIN packages p ON p.id = pr.package_id AND p.is_critical
)
SELECT
    (SELECT COUNT(*) FROM critical)                       AS critical_repos,
    COUNT(*)                                              AS repos_with_protocol,
    COUNT(*) FILTER (WHERE rp.declared)                   AS declared,
    ROUND(COUNT(*) FILTER (WHERE rp.declared)::numeric / NULLIF(COUNT(*), 0) * 100, 1) AS declared_pct
FROM repo_reporting_protocols rp
JOIN critical c ON c.id = rp.repo_id;

-- 3. Method sanity
-- Always returns four rows; healthy: every count is zero.
SELECT 'bad_type' AS problem, COUNT(*)
FROM repo_reporting_protocols rp,
     jsonb_array_elements(rp.methods) m
WHERE m->>'type' IS NULL
   OR m->>'type' NOT IN ('github-pvr','email','web-form','bounty-platform','security-txt','mailing-list')
UNION ALL
SELECT 'bad_status', COUNT(*)
FROM repo_reporting_protocols rp,
     jsonb_array_elements(rp.methods) m
WHERE m->>'status' IS NULL
   OR m->>'status' NOT IN ('preferred','accepted','fallback','prohibited')
UNION ALL
SELECT 'multiple_preferred', COUNT(*)
FROM (
    SELECT rp.repo_id
    FROM repo_reporting_protocols rp,
         jsonb_array_elements(rp.methods) m
    WHERE m->>'status' = 'preferred'
    GROUP BY rp.repo_id
    HAVING COUNT(*) > 1
) x
UNION ALL
SELECT 'inferred_marked_declared', COUNT(*)
FROM repo_reporting_protocols rp,
     jsonb_array_elements(rp.methods) m
WHERE rp.declared = false AND m->>'confidence' = 'declared';

-- 4. PVR flag consistency
-- Healthy: zero — pvr_enabled=false repos must carry no github-pvr method.
SELECT COUNT(*)
FROM repo_reporting_protocols rp
JOIN repos r ON r.id = rp.repo_id AND r.pvr_enabled = false
WHERE EXISTS (
    SELECT 1 FROM jsonb_array_elements(rp.methods) m WHERE m->>'type' = 'github-pvr');

-- 5. Ground-truth sample: 15 declared + 15 inferred for manual spot-check
(SELECT r.url, rp.declared, rp.methods
   FROM repo_reporting_protocols rp JOIN repos r ON r.id = rp.repo_id
  WHERE rp.declared ORDER BY rp.assembled_at DESC LIMIT 15)
UNION ALL
(SELECT r.url, rp.declared, rp.methods
   FROM repo_reporting_protocols rp JOIN repos r ON r.id = rp.repo_id
  WHERE NOT rp.declared AND jsonb_array_length(rp.methods) > 0
  ORDER BY rp.assembled_at DESC LIMIT 15);
