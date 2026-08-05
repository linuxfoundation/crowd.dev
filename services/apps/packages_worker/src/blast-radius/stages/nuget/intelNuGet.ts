import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'

import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { getVersionNumbers } from '@crowd/data-access-layer/src/packages/blastRadiusDependents'
import { findPackageId } from '@crowd/data-access-layer/src/packages/osv'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { fetchVersionList } from '../../../nuget/client'
import { isNuGetFetchError } from '../../../nuget/types'
import {
  NUGET_INTEL_SCHEMA,
  NUGET_INTEL_SYSTEM_PROMPT,
  buildNuGetIntelPrompt,
} from '../../agent/nugetPrompts'
import { runAnalysisAgent } from '../../agent/runner'
import { fetchPatch } from '../../clients/githubPatch'
import { downloadAndExtractNuGetSource } from '../../clients/nugetSource'
import {
  affectedEntriesForEcosystem,
  fetchOsvVuln,
  fixReferenceUrls,
} from '../../clients/osvClient'
import { toBareNuGetId } from '../../packageIdentifier'
import { ecosystemRangeEvents, highestVersion, versionsInRanges } from '../ecosystemVersions'
import { selectAdvisoryEntry } from '../selectAdvisoryEntry'

// OSV spells the NuGet ecosystem 'NuGet' (mixed case), unlike our DB's lowercase 'nuget'.
const OSV_NUGET_ECOSYSTEM = 'NuGet'

export async function runIntelStageNuGet(
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

    const nugetEntries = affectedEntriesForEcosystem(osv, OSV_NUGET_ECOSYSTEM)
    if (nugetEntries.length === 0) {
      throw new Error(`No NuGet entries found in advisory ${advisoryOsvId}`)
    }

    // Pick the NuGet entry the analysis was requested for; see selectAdvisoryEntry for
    // rejection rules on non-matching or omitted requests.
    const analysisDetail = await blastRadiusDal.getAnalysisDetail(qx, analysisId)
    const requestedId =
      analysisDetail?.package_name != null ? toBareNuGetId(analysisDetail.package_name) : null
    // NuGet package IDs are case-insensitive; match case-insensitively but resolve
    // to OSV's own canonical spelling below so the case-sensitive DB lookup succeeds.
    const { entry, relatedAffectedPackages } = selectAdvisoryEntry(
      nugetEntries,
      requestedId,
      (e) => e.package.name.toLowerCase() === requestedId?.toLowerCase(),
      advisoryOsvId,
    )

    const nugetId = entry.package.name
    const ecosystem = 'nuget'

    // Resolve vulnerable versions from OSV ranges first (NuGet OSV ranges are
    // ECOSYSTEM-typed, not SEMVER — same shape as Maven, see ecosystemVersions.ts).
    const ranges = ecosystemRangeEvents(entry)

    const dbPackageId = await findPackageId(qx, { ecosystem, namespace: null, name: nugetId })

    // The nuget.org registration index is the authoritative version list; fall back to
    // our own ingested `versions` rows (deps.dev) if the registry is unreachable/rate-limited
    // and the package is already known to us.
    const versionListResult = await fetchVersionList(nugetId)
    let allVersions: string[]
    if (!isNuGetFetchError(versionListResult)) {
      allVersions = versionListResult
    } else if (dbPackageId) {
      allVersions = await getVersionNumbers(qx, String(dbPackageId))
    } else {
      throw new Error(
        `Failed to fetch NuGet version list for ${nugetId} (${versionListResult.kind}) and package is not in our DB`,
      )
    }

    const vulnerableVersions = versionsInRanges('nuget', allVersions, ranges)

    const analyzed = highestVersion('nuget', vulnerableVersions)
    if (!analyzed) {
      throw new Error(`Could not determine analyzed version for ${nugetId}`)
    }

    const pkgsrcDir = fs.mkdtempSync(path.join(os.tmpdir(), 'nugetsrc-'))
    const patches: Record<string, string> = {}

    try {
      await downloadAndExtractNuGetSource(nugetId, analyzed, pkgsrcDir, onProgress)

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

      const agentPrompt = buildNuGetIntelPrompt(
        osv.id || advisoryOsvId,
        osv.aliases || [],
        osv.details || osv.summary || '',
        analyzed,
        patches,
      )

      const agentResult = await runAnalysisAgent({
        prompt: agentPrompt,
        systemPrompt: NUGET_INTEL_SYSTEM_PROMPT,
        cwd: pkgsrcDir,
        model: 'claude-opus-4-8',
        schema: NUGET_INTEL_SCHEMA,
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
        package: nugetId,
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
