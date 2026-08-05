import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'

import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { getVersionNumbers } from '@crowd/data-access-layer/src/packages/blastRadiusDependents'
import { findPackageId } from '@crowd/data-access-layer/src/packages/osv'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import {
  CARGO_INTEL_SCHEMA,
  CARGO_INTEL_SYSTEM_PROMPT,
  buildCargoIntelPrompt,
} from '../../agent/cargoPrompts'
import { runAnalysisAgent } from '../../agent/runner'
import { fetchPatch } from '../../clients/githubPatch'
import { downloadAndExtractTarball } from '../../clients/npmTarball'
import {
  affectedEntriesForEcosystem,
  fetchOsvVuln,
  fixReferenceUrls,
  semverRangeEvents,
} from '../../clients/osvClient'
import { crateSourceUrl, fetchCrateVersions } from '../../crates/registryClient'
import { toBareCargoName, toDbCargoName } from '../../packageIdentifier'
import { highestVersion, versionsInRanges } from '../../semverRange'
import { selectAdvisoryEntry } from '../selectAdvisoryEntry'

// OSV spells the Cargo ecosystem 'crates.io', unlike our DB's lowercase 'cargo' — see
// ADR-0001 §OSV "Ecosystem normalization" for the DB-side convention.
const OSV_CARGO_ECOSYSTEM = 'crates.io'
const CRATES_IO_FETCH_TIMEOUT_MS = 15_000

export async function runIntelStageCargo(
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

    const cargoEntries = affectedEntriesForEcosystem(osv, OSV_CARGO_ECOSYSTEM)
    if (cargoEntries.length === 0) {
      throw new Error(`No Cargo entries found in advisory ${advisoryOsvId}`)
    }

    // Pick the crate entry the analysis was requested for; see selectAdvisoryEntry for
    // rejection rules on non-matching or omitted requests.
    const analysisDetail = await blastRadiusDal.getAnalysisDetail(qx, analysisId)
    const requestedCrate = analysisDetail?.package_name
      ? toBareCargoName(analysisDetail.package_name)
      : null
    const { entry, relatedAffectedPackages } = selectAdvisoryEntry(
      cargoEntries,
      requestedCrate,
      (e) => e.package.name === requestedCrate,
      advisoryOsvId,
    )
    const crate = entry.package.name
    const ecosystem = 'cargo'

    // Resolve vulnerable versions from OSV ranges first (crates.io OSV ranges are
    // SEMVER-typed, same event shape as npm's/Go's).
    const ranges = semverRangeEvents(entry)

    const packageId = await findPackageId(qx, {
      ecosystem,
      namespace: null,
      name: toDbCargoName(crate),
    })

    // Use crates.io version list; fall back to our DB if unreachable and crate is known.
    const versionListResult = await fetchCrateVersions(crate, CRATES_IO_FETCH_TIMEOUT_MS)
    let allVersions: string[]
    if (Array.isArray(versionListResult)) {
      allVersions = versionListResult
    } else if (packageId !== null) {
      allVersions = await getVersionNumbers(qx, String(packageId))
    } else {
      throw new Error(
        `Failed to fetch versions for ${crate} (${versionListResult.message}) and crate is not in our DB`,
      )
    }

    const vulnerableVersions = versionsInRanges(allVersions, ranges)

    const analyzed = highestVersion(vulnerableVersions)
    if (!analyzed) {
      throw new Error(`Could not determine analyzed version for ${crate}`)
    }

    const pkgsrcDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cratesrc-'))
    const patches: Record<string, string> = {}

    try {
      await downloadAndExtractTarball(crateSourceUrl(crate, analyzed), pkgsrcDir)

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

      const agentPrompt = buildCargoIntelPrompt(
        osv.id || advisoryOsvId,
        osv.aliases || [],
        osv.details || osv.summary || '',
        analyzed,
        patches,
      )

      const agentResult = await runAnalysisAgent({
        prompt: agentPrompt,
        systemPrompt: CARGO_INTEL_SYSTEM_PROMPT,
        cwd: pkgsrcDir,
        model: 'claude-opus-4-8',
        schema: CARGO_INTEL_SCHEMA,
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
        package: crate,
        ecosystem,
        affectedRanges: ranges,
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
