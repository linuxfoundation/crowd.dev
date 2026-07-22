import { QueryExecutor } from '../queryExecutor'

// DAL for blast-radius analysis pipeline. All tables defined in
// backend/src/osspckgs/migrations/V1784700000__blast_radius_analyses.sql.

// ---- input shapes ----

export interface BlastRadiusAnalysisInput {
  id: string
  advisoryOsvId: string
  packageName: string | null
  ecosystem: string
  force: boolean
}

export interface SymbolSpecInput {
  analysisId: string
  vulnId: string
  aliases: string[]
  package: string
  ecosystem: string
  affectedRanges: Record<string, unknown>[]
  vulnerableVersions: string[]
  analyzedVersion: string
  relatedAffectedPackages: string[]
  vulnerableSymbols: Record<string, unknown>[]
  importSignatures: Record<string, unknown>
  exploitPreconditions: string
  reachabilityNotes: string
  confidence: number
  sources: string[]
  summary: string
}

export interface DependentInput {
  analysisId: string
  packageId: number | null
  name: string
  version: string | null
  downloads: number | null
  declaredRange: string | null
  dependencyKind: string | null
  rangeIncludesVuln: boolean | null
  rangeCheck: string | null
  tarballUrl: string | null
  excludedByRange: boolean
  exclusionReason: string | null
}

export interface VerdictInput {
  analysisId: string
  dependentId: number
  usesPackage: boolean
  importsVulnerableSymbol: boolean
  importStyle: string | null
  reachableVerdict: string
  confidence: number
  evidence: Record<string, unknown>[] | null
  reasoning: string | null
  model: string | null
  turnsUsed: number | null
  costUsd: number
}

export interface StageRunInput {
  analysisId: string
  stage: string
  status: string
  model: string | null
  startedAt?: string
}

// ---- result shapes ----

export interface AnalysisRow {
  id: string
  advisory_id: number | null
  package_id: number | null
  status: string
  total_cost_usd: number
}

export interface DependentRow {
  id: number
  analysis_id: string
  name: string
  excluded_by_range: boolean
  tarball_url: string | null
}

export interface AnalysisDetailRow {
  id: string
  advisory_osv_id: string
  package_name: string | null
  ecosystem: string
  status: string
  error: string | null
  candidates_considered: number | null
  started_at: string | null
  completed_at: string | null
}

export interface VerdictResultRow {
  name: string
  version: string | null
  downloads: number | null
  reachable_verdict: string
  confidence: number
  evidence: Record<string, unknown>[] | null
  reasoning: string | null
}

// ---- analysis lifecycle ----

export async function createAnalysis(
  qx: QueryExecutor,
  input: BlastRadiusAnalysisInput,
): Promise<string> {
  const row = await qx.selectOne(
    `
    INSERT INTO blast_radius_analyses
      (id, advisory_osv_id, package_name, ecosystem, force, status, started_at, updated_at)
    VALUES
      ($(id), $(advisoryOsvId), $(packageName), $(ecosystem), $(force), 'pending', NOW(), NOW())
    ON CONFLICT (id) DO UPDATE SET
      force = EXCLUDED.force,
      status = CASE
        WHEN blast_radius_analyses.status = 'pending' THEN 'pending'
        ELSE blast_radius_analyses.status
      END,
      updated_at = NOW()
    RETURNING id
    `,
    input,
  )
  return row.id as string
}

export async function markAnalysisRunning(qx: QueryExecutor, analysisId: string): Promise<void> {
  await qx.result(
    `
    UPDATE blast_radius_analyses
    SET status = 'running', started_at = NOW(), updated_at = NOW()
    WHERE id = $(analysisId) AND status = 'pending'
    `,
    { analysisId },
  )
}

export async function getAnalysis(
  qx: QueryExecutor,
  analysisId: string,
): Promise<AnalysisRow | null> {
  return qx.selectOneOrNone(
    `
    SELECT id, advisory_id, package_id, status, total_cost_usd
    FROM blast_radius_analyses
    WHERE id = $(analysisId)
    `,
    { analysisId },
  )
}

export async function getAnalysisDetail(
  qx: QueryExecutor,
  analysisId: string,
): Promise<AnalysisDetailRow | null> {
  return qx.selectOneOrNone(
    `
    SELECT
      id, advisory_osv_id, package_name, ecosystem, status, error,
      candidates_considered, started_at, completed_at
    FROM blast_radius_analyses
    WHERE id = $(analysisId)
    `,
    { analysisId },
  )
}

export async function setDependentsMeta(
  qx: QueryExecutor,
  analysisId: string,
  source: string,
  candidatesConsidered: number,
): Promise<void> {
  await qx.result(
    `
    UPDATE blast_radius_analyses
    SET dependents_source = $(source), candidates_considered = $(candidatesConsidered), updated_at = NOW()
    WHERE id = $(analysisId)
    `,
    { analysisId, source, candidatesConsidered },
  )
}

export async function resolveAdvisoryAndPackageIds(
  qx: QueryExecutor,
  analysisId: string,
  advisoryOsvId: string,
  packageId: number | null,
): Promise<void> {
  await qx.result(
    `
    UPDATE blast_radius_analyses bra
    SET
      advisory_id = COALESCE(a.id, bra.advisory_id),
      package_id = COALESCE($(packageId), bra.package_id),
      updated_at = NOW()
    FROM advisories a
    WHERE bra.id = $(analysisId)
      AND a.osv_id = $(advisoryOsvId)
    `,
    { analysisId, advisoryOsvId, packageId },
  )
}

export async function finalizeAnalysis(
  qx: QueryExecutor,
  analysisId: string,
  totalCostUsd: number,
): Promise<void> {
  await qx.result(
    `
    UPDATE blast_radius_analyses
    SET
      status = 'done',
      total_cost_usd = $(totalCostUsd),
      completed_at = NOW(),
      updated_at = NOW()
    WHERE id = $(analysisId)
    `,
    { analysisId, totalCostUsd },
  )
}

export async function failAnalysis(
  qx: QueryExecutor,
  analysisId: string,
  errorMessage: string,
): Promise<void> {
  await qx.result(
    `
    UPDATE blast_radius_analyses
    SET
      status = 'failed',
      error = $(errorMessage),
      completed_at = NOW(),
      updated_at = NOW()
    WHERE id = $(analysisId)
    `,
    { analysisId, errorMessage },
  )
}

// ---- symbol specs ----

export async function upsertSymbolSpec(qx: QueryExecutor, input: SymbolSpecInput): Promise<number> {
  const params = {
    ...input,
    affectedRanges: JSON.stringify(input.affectedRanges),
    vulnerableSymbols: JSON.stringify(input.vulnerableSymbols),
    importSignatures: JSON.stringify(input.importSignatures),
  }
  const row = await qx.selectOne(
    `
    INSERT INTO blast_radius_symbol_specs
      (analysis_id, vuln_id, aliases, package, ecosystem, affected_ranges,
       vulnerable_versions, analyzed_version, related_affected_packages,
       vulnerable_symbols, import_signatures, exploit_preconditions,
       reachability_notes, confidence, sources, summary, created_at)
    VALUES
      ($(analysisId), $(vulnId), $(aliases)::text[], $(package), $(ecosystem),
       $(affectedRanges)::jsonb, $(vulnerableVersions)::text[],
       $(analyzedVersion), $(relatedAffectedPackages)::text[],
       $(vulnerableSymbols)::jsonb, $(importSignatures)::jsonb,
       $(exploitPreconditions), $(reachabilityNotes), $(confidence),
       $(sources)::text[], $(summary), NOW())
    ON CONFLICT (analysis_id) DO UPDATE SET
      vuln_id = EXCLUDED.vuln_id,
      aliases = EXCLUDED.aliases,
      affected_ranges = EXCLUDED.affected_ranges,
      vulnerable_versions = EXCLUDED.vulnerable_versions,
      analyzed_version = EXCLUDED.analyzed_version,
      related_affected_packages = EXCLUDED.related_affected_packages,
      vulnerable_symbols = EXCLUDED.vulnerable_symbols,
      import_signatures = EXCLUDED.import_signatures,
      exploit_preconditions = EXCLUDED.exploit_preconditions,
      reachability_notes = EXCLUDED.reachability_notes,
      confidence = EXCLUDED.confidence,
      sources = EXCLUDED.sources,
      summary = EXCLUDED.summary
    RETURNING id
    `,
    params,
  )
  return row.id as number
}

export async function getSymbolSpec(
  qx: QueryExecutor,
  analysisId: string,
): Promise<Record<string, unknown> | null> {
  return qx.selectOneOrNone(
    `
    SELECT
      vuln_id, aliases, package, ecosystem, affected_ranges,
      vulnerable_versions, analyzed_version, related_affected_packages,
      vulnerable_symbols, import_signatures, exploit_preconditions,
      reachability_notes, confidence, sources, summary
    FROM blast_radius_symbol_specs
    WHERE analysis_id = $(analysisId)
    `,
    { analysisId },
  )
}

// ---- dependents ----

export async function insertDependents(
  qx: QueryExecutor,
  dependents: DependentInput[],
): Promise<void> {
  if (dependents.length === 0) return
  for (const dep of dependents) {
    await qx.result(
      `
      INSERT INTO blast_radius_dependents
        (analysis_id, package_id, name, version, downloads, declared_range,
         dependency_kind, range_includes_vuln, range_check, tarball_url,
         excluded_by_range, exclusion_reason, created_at)
      VALUES
        ($(analysisId), $(packageId), $(name), $(version), $(downloads),
         $(declaredRange), $(dependencyKind), $(rangeIncludesVuln),
         $(rangeCheck), $(tarballUrl), $(excludedByRange), $(exclusionReason), NOW())
      ON CONFLICT (analysis_id, name) DO UPDATE SET
        package_id = EXCLUDED.package_id,
        version = EXCLUDED.version,
        downloads = EXCLUDED.downloads,
        declared_range = EXCLUDED.declared_range,
        dependency_kind = EXCLUDED.dependency_kind,
        range_includes_vuln = EXCLUDED.range_includes_vuln,
        range_check = EXCLUDED.range_check,
        tarball_url = EXCLUDED.tarball_url,
        excluded_by_range = EXCLUDED.excluded_by_range,
        exclusion_reason = EXCLUDED.exclusion_reason
      `,
      dep,
    )
  }
}

export async function getDependentsNeedingVerdict(
  qx: QueryExecutor,
  analysisId: string,
): Promise<DependentRow[]> {
  return qx.select(
    `
    SELECT id, analysis_id, name, excluded_by_range, tarball_url
    FROM blast_radius_dependents
    WHERE analysis_id = $(analysisId)
      AND excluded_by_range = FALSE
      AND id NOT IN (SELECT dependent_id FROM blast_radius_verdicts WHERE analysis_id = $(analysisId))
    ORDER BY downloads DESC NULLS LAST
    `,
    { analysisId },
  )
}

// ---- verdicts ----

export async function upsertVerdict(qx: QueryExecutor, input: VerdictInput): Promise<number> {
  const params = {
    ...input,
    evidence: input.evidence === null ? null : JSON.stringify(input.evidence),
  }
  const row = await qx.selectOne(
    `
    INSERT INTO blast_radius_verdicts
      (analysis_id, dependent_id, uses_package, imports_vulnerable_symbol,
       import_style, reachable_verdict, confidence, evidence, reasoning,
       model, turns_used, cost_usd, created_at)
    VALUES
      ($(analysisId), $(dependentId), $(usesPackage), $(importsVulnerableSymbol),
       $(importStyle), $(reachableVerdict), $(confidence), $(evidence)::jsonb,
       $(reasoning), $(model), $(turnsUsed), $(costUsd), NOW())
    ON CONFLICT (dependent_id) DO UPDATE SET
      uses_package = EXCLUDED.uses_package,
      imports_vulnerable_symbol = EXCLUDED.imports_vulnerable_symbol,
      import_style = EXCLUDED.import_style,
      reachable_verdict = EXCLUDED.reachable_verdict,
      confidence = EXCLUDED.confidence,
      evidence = EXCLUDED.evidence,
      reasoning = EXCLUDED.reasoning,
      model = EXCLUDED.model,
      turns_used = EXCLUDED.turns_used,
      cost_usd = EXCLUDED.cost_usd
    RETURNING id
    `,
    params,
  )
  return row.id as number
}

export async function getVerdicts(
  qx: QueryExecutor,
  analysisId: string,
): Promise<Array<Record<string, unknown>>> {
  return qx.select(
    `
    SELECT
      dependent_id, uses_package, imports_vulnerable_symbol,
      import_style, reachable_verdict, confidence, evidence,
      reasoning, cost_usd, turns_used
    FROM blast_radius_verdicts
    WHERE analysis_id = $(analysisId)
    `,
    { analysisId },
  )
}

export async function getVerdictResults(
  qx: QueryExecutor,
  analysisId: string,
): Promise<VerdictResultRow[]> {
  return qx.select(
    `
    SELECT
      d.name, d.version, d.downloads,
      v.reachable_verdict, v.confidence, v.evidence, v.reasoning
    FROM blast_radius_verdicts v
    JOIN blast_radius_dependents d ON d.id = v.dependent_id
    WHERE v.analysis_id = $(analysisId)
    ORDER BY d.downloads DESC NULLS LAST
    `,
    { analysisId },
  )
}

// ---- stage runs (monitoring) ----

export async function startStageRun(qx: QueryExecutor, input: StageRunInput): Promise<number> {
  const params = { ...input, startedAt: input.startedAt ?? null }
  const row = await qx.selectOne(
    `
    INSERT INTO blast_radius_stage_runs
      (analysis_id, stage, status, model, started_at)
    VALUES
      ($(analysisId), $(stage), $(status), $(model), COALESCE($(startedAt), NOW()))
    ON CONFLICT (analysis_id, stage) DO UPDATE SET
      status = EXCLUDED.status,
      started_at = EXCLUDED.started_at
    RETURNING id
    `,
    params,
  )
  return row.id as number
}

export async function completeStageRun(
  qx: QueryExecutor,
  analysisId: string,
  stage: string,
  durationMs: number,
  costUsd: number,
): Promise<void> {
  await qx.result(
    `
    UPDATE blast_radius_stage_runs
    SET
      status = 'succeeded',
      completed_at = NOW(),
      duration_ms = $(durationMs),
      cost_usd = $(costUsd)
    WHERE analysis_id = $(analysisId) AND stage = $(stage)
    `,
    { analysisId, stage, durationMs, costUsd },
  )
}

export async function failStageRun(
  qx: QueryExecutor,
  analysisId: string,
  stage: string,
  durationMs: number,
  errorMessage: string,
): Promise<void> {
  await qx.result(
    `
    UPDATE blast_radius_stage_runs
    SET
      status = 'failed',
      completed_at = NOW(),
      duration_ms = $(durationMs),
      error = $(errorMessage)
    WHERE analysis_id = $(analysisId) AND stage = $(stage)
    `,
    { analysisId, stage, durationMs, errorMessage },
  )
}

export async function getStageRunStatus(
  qx: QueryExecutor,
  analysisId: string,
  stage: string,
): Promise<string | null> {
  const row = await qx.selectOneOrNone(
    `
    SELECT status
    FROM blast_radius_stage_runs
    WHERE analysis_id = $(analysisId) AND stage = $(stage)
    `,
    { analysisId, stage },
  )
  return row ? (row.status as string) : null
}

export async function getStageRunsCost(qx: QueryExecutor, analysisId: string): Promise<number> {
  const row = await qx.selectOne(
    `
    SELECT COALESCE(SUM(cost_usd), 0) as total_cost
    FROM blast_radius_stage_runs
    WHERE analysis_id = $(analysisId)
    `,
    { analysisId },
  )
  return (row.total_cost as number) || 0
}
