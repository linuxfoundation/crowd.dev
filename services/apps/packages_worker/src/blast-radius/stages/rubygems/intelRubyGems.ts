import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'

import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { getVersionNumbers } from '@crowd/data-access-layer/src/packages/blastRadiusDependents'
import { findPackageId } from '@crowd/data-access-layer/src/packages/osv'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { fetchVersions } from '../../../rubygems/client'
import { isRubyGemsFetchError } from '../../../rubygems/types'
import {
  RUBYGEMS_INTEL_SCHEMA,
  RUBYGEMS_INTEL_SYSTEM_PROMPT,
  buildRubyGemsIntelPrompt,
} from '../../agent/rubygemsPrompts'
import { runAnalysisAgent } from '../../agent/runner'
import { fetchPatch } from '../../clients/githubPatch'
import {
  affectedEntriesForEcosystem,
  fetchOsvVuln,
  fixReferenceUrls,
} from '../../clients/osvClient'
import { downloadAndExtractRubyGemsSource } from '../../clients/rubygemsSource'
import { toBareGemName } from '../../packageIdentifier'
import { ecosystemRangeEvents, highestVersion, versionsInRanges } from '../ecosystemVersions'
import { selectAdvisoryEntry } from '../selectAdvisoryEntry'

// OSV spells the RubyGems ecosystem 'RubyGems' (mixed case), unlike our DB's lowercase 'rubygems'.
const OSV_RUBYGEMS_ECOSYSTEM = 'RubyGems'

export async function runIntelStageRubyGems(
  qx: QueryExecutor,
  analysisId: string,
  advisoryOsvId: string,
  onProgress?: () => void,
): Promise<void> {
  const startTime = Date.now()

  try {
    // Check if already done — avoid clobbering a succeeded stage_run's status/started_at
    // on a redundant re-invocation (startStageRun's ON CONFLICT always overwrites status).
    const existingStatus = await blastRadiusDal.getStageRunStatus(qx, analysisId, 'intel')
    if (existingStatus === 'succeeded') {
      return
    }

    await blastRadiusDal.startStageRun(qx, {
      analysisId,
      stage: 'intel',
      status: 'running',
      model: 'claude-opus-4-8',
    })

    const osv = await fetchOsvVuln(advisoryOsvId)

    const rubygemsEntries = affectedEntriesForEcosystem(osv, OSV_RUBYGEMS_ECOSYSTEM)
    if (rubygemsEntries.length === 0) {
      throw new Error(`No RubyGems entries found in advisory ${advisoryOsvId}`)
    }

    // Pick the RubyGems entry the analysis was requested for; see selectAdvisoryEntry for
    // rejection rules on non-matching or omitted requests.
    const analysisDetail = await blastRadiusDal.getAnalysisDetail(qx, analysisId)
    const requestedName =
      analysisDetail?.package_name != null ? toBareGemName(analysisDetail.package_name) : null
    const { entry, relatedAffectedPackages } = selectAdvisoryEntry(
      rubygemsEntries,
      requestedName,
      (e) => e.package.name === requestedName,
      advisoryOsvId,
    )

    const gemName = entry.package.name
    const ecosystem = 'rubygems'

    // Resolve vulnerable versions from OSV ranges first (RubyGems OSV ranges are
    // ECOSYSTEM-typed, not SEMVER — same shape as Maven/Go/NuGet, see ecosystemVersions.ts).
    const ranges = ecosystemRangeEvents(entry)

    const dbPackageId = await findPackageId(qx, {
      ecosystem,
      namespace: null,
      name: gemName,
    })

    // rubygems.org is the authoritative version list; fall back to our own ingested
    // `versions` rows (deps.dev/rubygems sync) if the registry is unreachable/rate-limited
    // and the package is already known to us.
    const versionsResult = await fetchVersions(gemName)
    let allVersions: string[]
    if (!isRubyGemsFetchError(versionsResult)) {
      allVersions = versionsResult.map((v) => v.number)
    } else if (dbPackageId) {
      allVersions = await getVersionNumbers(qx, String(dbPackageId))
    } else {
      throw new Error(
        `Failed to fetch RubyGems version list for ${gemName} (${versionsResult.kind}) and package is not in our DB`,
      )
    }

    const vulnerableVersions = versionsInRanges('rubygems', allVersions, ranges)

    const analyzed = highestVersion('rubygems', vulnerableVersions)
    if (!analyzed) {
      throw new Error(`Could not determine analyzed version for ${gemName}`)
    }

    const pkgsrcDir = fs.mkdtempSync(path.join(os.tmpdir(), 'gemsrc-'))
    const patches: Record<string, string> = {}

    try {
      await downloadAndExtractRubyGemsSource(gemName, analyzed, pkgsrcDir)
      onProgress?.()

      const patchUrls = fixReferenceUrls(osv)
      for (const url of patchUrls.slice(0, 3)) {
        try {
          const patchText = await fetchPatch(url)
          const slug = new URL(url).pathname.split('/').filter(Boolean).join('-')
          patches[slug] = patchText
        } catch {
          // Ignore patch fetch errors
        }
      }

      const agentPrompt = buildRubyGemsIntelPrompt(
        osv.id || advisoryOsvId,
        osv.aliases || [],
        osv.details || osv.summary || '',
        analyzed,
        patches,
      )

      const agentResult = await runAnalysisAgent({
        prompt: agentPrompt,
        systemPrompt: RUBYGEMS_INTEL_SYSTEM_PROMPT,
        cwd: pkgsrcDir,
        model: 'claude-opus-4-8',
        schema: RUBYGEMS_INTEL_SCHEMA,
        maxTurns: 15,
        timeoutMs: 600_000,
        onProgress,
      })

      if (agentResult.isError || !agentResult.structuredOutput) {
        throw new Error(`Agent failed: ${agentResult.errorMessage}`)
      }

      const output = agentResult.structuredOutput
      await blastRadiusDal.upsertSymbolSpec(qx, {
        analysisId,
        vulnId: osv.id || advisoryOsvId,
        aliases: osv.aliases || [],
        package: gemName,
        ecosystem,
        affectedRanges: ranges as unknown as Record<string, unknown>[],
        vulnerableVersions,
        analyzedVersion: analyzed,
        relatedAffectedPackages,
        vulnerableSymbols: (output.vulnerable_symbols || []) as Record<string, unknown>[],
        importSignatures: (output.import_signatures || {}) as Record<string, unknown>,
        exploitPreconditions: String(output.exploit_preconditions || ''),
        reachabilityNotes: String(output.reachability_notes || ''),
        confidence: Number(output.confidence ?? 0.5),
        sources: [advisoryOsvId],
        summary: String(output.summary || ''),
      })

      await blastRadiusDal.resolveAdvisoryAndPackageIds(qx, analysisId, advisoryOsvId, dbPackageId)

      const duration = Date.now() - startTime
      await blastRadiusDal.completeStageRun(qx, analysisId, 'intel', duration, agentResult.costUsd)
    } finally {
      fs.rmSync(pkgsrcDir, { recursive: true, force: true })
    }
  } catch (err) {
    const duration = Date.now() - startTime
    const errorMsg = err instanceof Error ? err.message : String(err)
    await blastRadiusDal.failStageRun(qx, analysisId, 'intel', duration, errorMsg)
    throw err
  }
}
