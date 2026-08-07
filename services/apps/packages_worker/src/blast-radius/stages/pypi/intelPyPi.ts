import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'

import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { getVersionNumbers } from '@crowd/data-access-layer/src/packages/blastRadiusDependents'
import { findPackageIdByPurl } from '@crowd/data-access-layer/src/packages/osv'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { fetchProject } from '../../../pypi/fetchProject'
import { isFetchError } from '../../../pypi/types'
import {
  PYPI_INTEL_SCHEMA,
  PYPI_INTEL_SYSTEM_PROMPT,
  buildPyPiIntelPrompt,
} from '../../agent/pypiPrompts'
import { runAnalysisAgent } from '../../agent/runner'
import { fetchPatch } from '../../clients/githubPatch'
import {
  affectedEntriesForEcosystem,
  fetchOsvVuln,
  fixReferenceUrls,
} from '../../clients/osvClient'
import { downloadAndExtractPypiSource } from '../../clients/pypiSource'
import { toBarePypiName, toPypiNormalizedName } from '../../packageIdentifier'
import { ecosystemRangeEvents, highestVersion, versionsInRanges } from '../ecosystemVersions'
import { selectAdvisoryEntry } from '../selectAdvisoryEntry'

// OSV spells the PyPI ecosystem 'PyPI' (mixed case), unlike our DB's lowercase 'pypi' —
// see ADR-0001 §OSV "Ecosystem normalization" for the DB-side convention.
const OSV_PYPI_ECOSYSTEM = 'PyPI'

export async function runIntelStagePyPi(
  qx: QueryExecutor,
  analysisId: string,
  advisoryOsvId: string,
  onProgress?: () => void,
): Promise<void> {
  const startTime = Date.now()

  try {
    // Guard on stage_run status to avoid stuck failed/running state if intel crashes
    // between upsertSymbolSpec and completeStageRun.
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

    const pypiEntries = affectedEntriesForEcosystem(osv, OSV_PYPI_ECOSYSTEM)
    if (pypiEntries.length === 0) {
      throw new Error(`No PyPI entries found in advisory ${advisoryOsvId}`)
    }

    // Pick the project entry the analysis was requested for; see selectAdvisoryEntry for
    // rejection rules on non-matching or omitted requests.
    const analysisDetail = await blastRadiusDal.getAnalysisDetail(qx, analysisId)
    const requestedName = analysisDetail?.package_name
      ? toBarePypiName(analysisDetail.package_name)
      : null
    const requestedNormalized = requestedName !== null ? toPypiNormalizedName(requestedName) : null
    const { entry, relatedAffectedPackages } = selectAdvisoryEntry(
      pypiEntries,
      requestedName,
      (e) => toPypiNormalizedName(e.package.name) === requestedNormalized,
      advisoryOsvId,
    )
    const project = entry.package.name
    const normalizedName = toPypiNormalizedName(project)
    const ecosystem = 'pypi'

    // Resolve vulnerable versions from OSV ranges first (PyPI OSV ranges are
    // ECOSYSTEM-typed, not SEMVER — PEP 440 versions don't follow semver ordering).
    const ranges = ecosystemRangeEvents(entry)

    // packages.purl is always normalized; packages.name can drift from canonical PEP 503.
    // See: blast-radius PyPI plan's name-casing section.
    const purl = `pkg:pypi/${normalizedName}`
    const packageId = await findPackageIdByPurl(qx, purl)

    // pypi.org's JSON API is authoritative; fall back to ingested versions if unreachable.
    const projectResult = await fetchProject(normalizedName)
    let allVersions: string[]
    if (!isFetchError(projectResult)) {
      allVersions = Object.keys(projectResult.releases ?? {})
    } else if (packageId !== null) {
      allVersions = await getVersionNumbers(qx, packageId)
    } else {
      throw new Error(
        `Failed to fetch PyPI versions for ${project} (${projectResult.message}) and project is not in our DB`,
      )
    }

    const vulnerableVersions = versionsInRanges(ecosystem, allVersions, ranges)

    const analyzed = highestVersion(ecosystem, vulnerableVersions)
    if (!analyzed) {
      throw new Error(`Could not determine analyzed version for ${project}`)
    }

    const pkgsrcDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pypisrc-'))
    const patches: Record<string, string> = {}

    try {
      await downloadAndExtractPypiSource(normalizedName, analyzed, pkgsrcDir)

      const patchUrls = fixReferenceUrls(osv)
      const patchResults = await Promise.allSettled(
        patchUrls.slice(0, 3).map(async (url) => {
          const patchText = await fetchPatch(url)
          const slug = new URL(url).pathname.split('/').filter(Boolean).join('-')
          return { slug, patchText }
        }),
      )
      patchResults.forEach((result) => {
        if (result.status === 'fulfilled') {
          patches[result.value.slug] = result.value.patchText
        }
      })

      const agentPrompt = buildPyPiIntelPrompt(
        osv.id || advisoryOsvId,
        osv.aliases || [],
        osv.details || osv.summary || '',
        analyzed,
        patches,
      )

      const agentResult = await runAnalysisAgent({
        prompt: agentPrompt,
        systemPrompt: PYPI_INTEL_SYSTEM_PROMPT,
        cwd: pkgsrcDir,
        model: 'claude-opus-4-8',
        schema: PYPI_INTEL_SCHEMA,
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
        package: project,
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

      await blastRadiusDal.resolveAdvisoryAndPackageIds(qx, analysisId, advisoryOsvId, packageId)

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
