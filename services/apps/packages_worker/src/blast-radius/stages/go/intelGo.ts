import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'

import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { getVersionNumbers } from '@crowd/data-access-layer/src/packages/blastRadiusDependents'
import { findPackageId } from '@crowd/data-access-layer/src/packages/osv'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { fetchVersionList } from '../../../go/proxyClient'
import { GO_INTEL_SCHEMA, GO_INTEL_SYSTEM_PROMPT, buildGoIntelPrompt } from '../../agent/goPrompts'
import { runAnalysisAgent } from '../../agent/runner'
import { fetchPatch } from '../../clients/githubPatch'
import { downloadAndExtractGoModule } from '../../clients/goModuleZip'
import {
  affectedEntriesForEcosystem,
  fetchOsvVuln,
  fixReferenceUrls,
  semverRangeEvents,
} from '../../clients/osvClient'
import { highestVersion, versionsInRanges } from '../../semverRange'

// OSV spells the Go ecosystem 'Go' (capital), unlike our DB's lowercase 'go' — see
// ADR-0001 §OSV "Ecosystem normalization" for the DB-side convention.
const OSV_GO_ECOSYSTEM = 'Go'
const GO_PROXY_FETCH_TIMEOUT_MS = 15_000

// Go module paths (e.g. "github.com/pubnub/go/v7") are already bare — unlike npm,
// there's no '@scope/name' packing to undo, only an optional purl wrapper to strip.
function toBareGoModule(input: string): string {
  const decoded = decodeURIComponent(input)
  return decoded.startsWith('pkg:golang/') ? decoded.slice('pkg:golang/'.length) : decoded
}

export async function runIntelStageGo(
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

    await blastRadiusDal.startStageRun(qx, {
      analysisId,
      stage: 'intel',
      status: 'running',
      model: 'claude-opus-4-8',
    })

    const osv = await fetchOsvVuln(advisoryOsvId)

    const goEntries = affectedEntriesForEcosystem(osv, OSV_GO_ECOSYSTEM)
    if (goEntries.length === 0) {
      throw new Error(`No Go entries found in advisory ${advisoryOsvId}`)
    }

    // Multi-module advisories list one Go entry per affected module — pick the one the
    // analysis was actually requested for, falling back to the first entry otherwise.
    const analysisDetail = await blastRadiusDal.getAnalysisDetail(qx, analysisId)
    const requestedModule = analysisDetail?.package_name
      ? toBareGoModule(analysisDetail.package_name)
      : null
    const entry =
      (requestedModule && goEntries.find((e) => e.package.name === requestedModule)) || goEntries[0]
    const module_ = entry.package.name
    const ecosystem = 'go'
    const relatedAffectedPackages = goEntries
      .map((e) => e.package.name)
      .filter((name) => name !== module_)

    // Resolve vulnerable versions from OSV ranges first (Go OSV ranges are SEMVER-typed,
    // same event shape as npm's).
    const ranges = semverRangeEvents(entry)

    const packageId = await findPackageId(qx, { ecosystem, namespace: null, name: module_ })

    // GOPROXY's @v/list is the authoritative version list; fall back to our own
    // ingested `versions` rows (deps.dev) if GOPROXY is unreachable/rate-limited and
    // the module is already known to us.
    const versionListResult = await fetchVersionList(module_, GO_PROXY_FETCH_TIMEOUT_MS)
    let allVersions: string[]
    if (Array.isArray(versionListResult)) {
      allVersions = versionListResult
    } else if (packageId) {
      allVersions = await getVersionNumbers(qx, String(packageId))
    } else {
      throw new Error(
        `Failed to fetch @v/list for ${module_} (${versionListResult.message}) and module is not in our DB`,
      )
    }

    const vulnerableVersions = versionsInRanges(allVersions, ranges)

    const analyzed = highestVersion(vulnerableVersions)
    if (!analyzed) {
      throw new Error(`Could not determine analyzed version for ${module_}`)
    }

    const pkgsrcDir = fs.mkdtempSync(path.join(os.tmpdir(), 'gomodsrc-'))
    const patches: Record<string, string> = {}

    try {
      await downloadAndExtractGoModule(module_, analyzed, pkgsrcDir)

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

      const agentPrompt = buildGoIntelPrompt(
        osv.id || advisoryOsvId,
        osv.aliases || [],
        osv.details || osv.summary || '',
        analyzed,
        patches,
      )

      const agentResult = await runAnalysisAgent({
        prompt: agentPrompt,
        systemPrompt: GO_INTEL_SYSTEM_PROMPT,
        cwd: pkgsrcDir,
        model: 'claude-opus-4-8',
        schema: GO_INTEL_SCHEMA,
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
        package: module_,
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
