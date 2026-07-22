import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'

import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { findPackageId } from '@crowd/data-access-layer/src/packages/osv'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { fetchPackument } from '../../npm/fetchPackument'
import { parseNpmName } from '../../npm/normalize'
import { isFetchError } from '../../npm/types'
import { INTEL_SCHEMA, INTEL_SYSTEM_PROMPT, buildIntelPrompt } from '../agent/prompts'
import { runAnalysisAgent } from '../agent/runner'
import { fetchPatch } from '../clients/githubPatch'
import { downloadAndExtractTarball } from '../clients/npmTarball'
import {
  affectedNpmEntries,
  fetchOsvVuln,
  fixReferenceUrls,
  semverRangeEvents,
} from '../clients/osvClient'
import { asNpmVersionManifest } from '../npmManifest'
import { toBareNpmName } from '../packageIdentifier'
import { highestVersion, versionsInRanges } from '../semverRange'

export async function runIntelStage(
  qx: QueryExecutor,
  analysisId: string,
  advisoryOsvId: string,
  onProgress?: () => void,
): Promise<void> {
  const startTime = Date.now()

  try {
    // Check if already done. Guard on the stage_run's own status rather than symbol-spec
    // presence — a crash between upsertSymbolSpec and completeStageRun would otherwise
    // leave the stage_run stuck failed/running forever while retries skip past it.
    const existingStatus = await blastRadiusDal.getStageRunStatus(qx, analysisId, 'intel')
    if (existingStatus === 'succeeded') {
      return
    }

    // Start stage run record
    await blastRadiusDal.startStageRun(qx, {
      analysisId,
      stage: 'intel',
      status: 'running',
      model: 'claude-opus-4-8',
    })

    // Fetch OSV record
    const osv = await fetchOsvVuln(advisoryOsvId)

    const npmEntries = affectedNpmEntries(osv)
    if (npmEntries.length === 0) {
      throw new Error(`No npm entries found in advisory ${advisoryOsvId}`)
    }

    // Multi-package advisories list one npm entry per affected package — pick the one
    // the analysis was actually requested for, falling back to the first entry when no
    // specific package was requested (analysis-wide advisory scan). The request accepts
    // either a bare name or a full purl (see blastRadiusJobRequestSchema), but OSV entries
    // are always bare names, so the requested package must be normalized before comparing.
    const analysisDetail = await blastRadiusDal.getAnalysisDetail(qx, analysisId)
    const requestedPackage = analysisDetail?.package_name
      ? toBareNpmName(analysisDetail.package_name)
      : null
    const entry =
      (requestedPackage && npmEntries.find((e) => e.package.name === requestedPackage)) ||
      npmEntries[0]
    const package_ = entry.package.name
    const ecosystem = entry.package.ecosystem
    const relatedAffectedPackages = npmEntries
      .map((e) => e.package.name)
      .filter((name) => name !== package_)

    // Fetch the registry packument so vulnerable-version resolution runs against versions
    // npm actually published, not just the OSV range's introduced/fixed boundary strings —
    // most advisories only list range boundaries, so that set is typically incomplete and
    // silently drops real published versions from vulnerableVersions/analyzed.
    const packument = await fetchPackument(package_)
    if (isFetchError(packument)) {
      throw new Error(`Failed to fetch npm packument for ${package_}: ${packument.message}`)
    }
    const allVersions = Object.keys(packument.versions || {})

    // Resolve vulnerable versions from ranges
    const ranges = semverRangeEvents(entry)
    const vulnerableVersions = versionsInRanges(allVersions, ranges)

    const analyzed = highestVersion(vulnerableVersions)
    if (!analyzed) {
      throw new Error(`Could not determine analyzed version for ${package_}`)
    }

    // Download pkgsrc and patches
    const pkgsrcDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pkgsrc-'))
    const patches: Record<string, string> = {}

    try {
      // Download package source (using npm registry)
      const versionData = packument.versions?.[analyzed]
      const tarballUrl = versionData ? asNpmVersionManifest(versionData).dist?.tarball : undefined
      if (!tarballUrl) {
        throw new Error(`No tarball URL found for ${package_}@${analyzed}`)
      }
      await downloadAndExtractTarball(tarballUrl, pkgsrcDir)

      // Fetch up to 3 patches from fix references
      const patchUrls = fixReferenceUrls(osv)
      for (const url of patchUrls.slice(0, 3)) {
        try {
          const patchText = await fetchPatch(url)
          const slug = url.split('/').slice(-2).join('-')
          patches[slug] = patchText
        } catch {
          // Ignore patch fetch errors
        }
      }

      // Run intelligence agent
      const agentPrompt = buildIntelPrompt(
        osv.id || advisoryOsvId,
        osv.aliases || [],
        osv.details || osv.summary || '',
        analyzed,
        patches,
      )

      const agentResult = await runAnalysisAgent({
        prompt: agentPrompt,
        systemPrompt: INTEL_SYSTEM_PROMPT,
        cwd: pkgsrcDir,
        model: 'claude-opus-4-8',
        schema: INTEL_SCHEMA,
        maxTurns: 15,
        timeoutMs: 600_000,
        onProgress,
      })

      if (agentResult.isError || !agentResult.structuredOutput) {
        throw new Error(`Agent failed: ${agentResult.errorMessage}`)
      }

      // Persist symbol spec
      const output = agentResult.structuredOutput
      await blastRadiusDal.upsertSymbolSpec(qx, {
        analysisId,
        vulnId: osv.id || advisoryOsvId,
        aliases: osv.aliases || [],
        package: package_,
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

      // Resolve advisory_id and package_id (null if the package isn't in our DB yet —
      // resolveAdvisoryAndPackageIds COALESCEs, so this never clobbers an existing value)
      const { namespace, name } = parseNpmName(package_)
      const packageId = await findPackageId(qx, { ecosystem, namespace, name })
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
