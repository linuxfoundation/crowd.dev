import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import { promisify } from 'util'

import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import {
  REACHABILITY_PROMPT,
  SymbolSpec,
  VERDICT_SCHEMA,
  buildReachabilitySystemPrompt,
} from '../agent/prompts'
import { runAnalysisAgent } from '../agent/runner'
import { downloadAndExtractTarball } from '../clients/npmTarball'

const mkdtemp = promisify(fs.mkdtemp)

const MAX_ATTEMPTS = 3
const RETRY_BACKOFF_BASE = 15_000 // 15 seconds

// blastRadiusDal.getSymbolSpec returns the raw DB row (JSONB columns as
// unknown); prompts.ts's SymbolSpec is the shape the reachability prompt
// builder actually reads. Field names line up 1:1 with the query's SELECT list.
function toPromptSymbolSpec(row: Record<string, unknown>): SymbolSpec {
  return {
    vuln_id: String(row.vuln_id ?? ''),
    package: String(row.package ?? ''),
    summary: String(row.summary ?? ''),
    vulnerable_symbols: (row.vulnerable_symbols ?? []) as SymbolSpec['vulnerable_symbols'],
    import_signatures: (row.import_signatures ?? {}) as SymbolSpec['import_signatures'],
    exploit_preconditions: String(row.exploit_preconditions ?? ''),
    reachability_notes: String(row.reachability_notes ?? ''),
    confidence: Number(row.confidence ?? 0),
  }
}

export async function runReachabilityStage(
  qx: QueryExecutor,
  analysisId: string,
  onProgress?: () => void,
): Promise<void> {
  const startTime = Date.now()

  try {
    // Check if already done — avoid clobbering a succeeded stage_run's status/started_at
    // on a redundant re-invocation (startStageRun's ON CONFLICT always overwrites status).
    const existingStatus = await blastRadiusDal.getStageRunStatus(qx, analysisId, 'reachability')
    if (existingStatus === 'succeeded') {
      return
    }

    // Start stage run record
    await blastRadiusDal.startStageRun(qx, {
      analysisId,
      stage: 'reachability',
      status: 'running',
      model: 'claude-sonnet-5',
    })

    // Get symbol spec and dependents needing verdict
    const specRow = await blastRadiusDal.getSymbolSpec(qx, analysisId)
    if (!specRow) {
      throw new Error('Symbol spec not found')
    }
    const spec = toPromptSymbolSpec(specRow)

    const dependents = await blastRadiusDal.getDependentsNeedingVerdict(qx, analysisId)

    // Process with concurrency limit (4)
    const concurrency = 4
    const queue = [...dependents]
    const results: { cost: number; count: number } = { cost: 0, count: 0 }

    const upsertErrorVerdict = (dependentId: number, reasoning: string, model: string | null) =>
      blastRadiusDal.upsertVerdict(qx, {
        analysisId,
        dependentId,
        usesPackage: false,
        importsVulnerableSymbol: false,
        importStyle: null,
        reachableVerdict: 'unclear',
        confidence: 0,
        evidence: null,
        reasoning,
        model,
        turnsUsed: null,
        costUsd: 0,
      })

    const processOne = async (dep: blastRadiusDal.DependentRow): Promise<void> => {
      if (!dep.tarball_url) {
        await upsertErrorVerdict(dep.id, 'No tarball URL available', null)
        return
      }

      // Create temp dir for this dependent
      const depDir = await mkdtemp(path.join(os.tmpdir(), `dep-${dep.name}-`))

      try {
        // Download and extract
        await downloadAndExtractTarball(dep.tarball_url, depDir)

        // Try agent up to MAX_ATTEMPTS times with exponential backoff
        for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
          try {
            const systemPrompt = buildReachabilitySystemPrompt(spec)

            const agentResult = await runAnalysisAgent({
              prompt: REACHABILITY_PROMPT,
              systemPrompt,
              cwd: depDir,
              model: 'claude-sonnet-5',
              schema: VERDICT_SCHEMA,
              maxTurns: 15,
              timeoutMs: 600_000,
            })

            if (!agentResult.isError && agentResult.structuredOutput) {
              const output = agentResult.structuredOutput

              await blastRadiusDal.upsertVerdict(qx, {
                analysisId,
                dependentId: dep.id,
                usesPackage: Boolean(output.uses_package),
                importsVulnerableSymbol: Boolean(output.imports_vulnerable_symbol),
                importStyle: String(output.import_style || 'none'),
                reachableVerdict: String(output.reachable_verdict || 'unclear'),
                confidence: Number(output.confidence || 0),
                evidence: (output.evidence as unknown as Record<string, unknown>[]) ?? null,
                reasoning: String(output.reasoning || ''),
                model: 'claude-sonnet-5',
                turnsUsed: agentResult.numTurns,
                costUsd: agentResult.costUsd || 0,
              })

              results.cost += agentResult.costUsd || 0
              results.count++
              return
            }

            // Agent error; retry if not last attempt
            if (attempt === MAX_ATTEMPTS) {
              throw new Error(agentResult.errorMessage || 'Agent failed')
            }
          } catch (err) {
            if (attempt === MAX_ATTEMPTS) {
              // Last attempt; save error verdict
              await upsertErrorVerdict(
                dep.id,
                `Agent failed: ${err instanceof Error ? err.message : String(err)}`,
                'claude-sonnet-5',
              )
              return
            }

            // Backoff before retry
            const delayMs = RETRY_BACKOFF_BASE * attempt
            await new Promise((resolve) => setTimeout(resolve, delayMs))
          }
        }
      } finally {
        // Clean up temp dir
        fs.rmSync(depDir, { recursive: true, force: true })
        onProgress?.()
      }
    }

    // Process queue with concurrency limit
    while (queue.length > 0) {
      const batch = queue.splice(0, concurrency)
      await Promise.all(batch.map(processOne))
    }

    const duration = Date.now() - startTime
    await blastRadiusDal.completeStageRun(qx, analysisId, 'reachability', duration, results.cost)
  } catch (err) {
    const duration = Date.now() - startTime
    const errorMsg = err instanceof Error ? err.message : String(err)
    await blastRadiusDal.failStageRun(qx, analysisId, 'reachability', duration, errorMsg)
    throw err
  }
}
