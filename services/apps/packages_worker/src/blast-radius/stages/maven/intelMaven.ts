import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'

import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { getVersionNumbers } from '@crowd/data-access-layer/src/packages/blastRadiusDependents'
import { findPackageId } from '@crowd/data-access-layer/src/packages/osv'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { resolveVersionsList } from '../../../maven/metadata'
import { resolveBlastRadiusMavenBaseUrl } from '../../../maven/registry'
import {
  MAVEN_INTEL_SCHEMA,
  MAVEN_INTEL_SYSTEM_PROMPT,
  buildMavenIntelPrompt,
} from '../../agent/mavenPrompts'
import { runAnalysisAgent } from '../../agent/runner'
import { fetchPatch } from '../../clients/githubPatch'
import { downloadAndExtractMavenSources } from '../../clients/mavenSourcesJar'
import {
  affectedEntriesForEcosystem,
  fetchOsvVuln,
  fixReferenceUrls,
} from '../../clients/osvClient'
import { toBareMavenCoordinate } from '../../packageIdentifier'

import { highestVersion, mavenRangeEvents, versionsInRanges } from './mavenVersions'

// OSV spells the Maven ecosystem 'Maven' (capital), unlike our DB's lowercase 'maven' —
// see ADR-0001 §OSV "Ecosystem normalization" for the DB-side convention.
const OSV_MAVEN_ECOSYSTEM = 'Maven'

export async function runIntelStageMaven(
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

    const mavenEntries = affectedEntriesForEcosystem(osv, OSV_MAVEN_ECOSYSTEM)
    if (mavenEntries.length === 0) {
      throw new Error(`No Maven entries found in advisory ${advisoryOsvId}`)
    }

    // Multi-artifact advisories list one Maven entry per affected artifact — pick the one
    // the analysis was actually requested for, falling back to the first entry otherwise.
    const analysisDetail = await blastRadiusDal.getAnalysisDetail(qx, analysisId)
    const requested = analysisDetail?.package_name
      ? toBareMavenCoordinate(analysisDetail.package_name)
      : null
    const entry =
      (requested &&
        mavenEntries.find((e) => {
          const coord = toBareMavenCoordinate(e.package.name)
          return coord.groupId === requested.groupId && coord.artifactId === requested.artifactId
        })) ||
      mavenEntries[0]

    const { groupId, artifactId } = toBareMavenCoordinate(entry.package.name)
    const coordinate = `${groupId}:${artifactId}`
    const ecosystem = 'maven'
    const relatedAffectedPackages = mavenEntries
      .map((e) => e.package.name)
      .filter((name) => name !== entry.package.name)

    // Resolve vulnerable versions from OSV ranges first (Maven OSV ranges are
    // ECOSYSTEM-typed, not SEMVER — Maven versions don't follow semver ordering).
    const ranges = mavenRangeEvents(entry)

    const packageId = await findPackageId(qx, { ecosystem, namespace: groupId, name: artifactId })

    // maven-metadata.xml is the authoritative version list; fall back to our own
    // ingested `versions` rows (deps.dev) if the registry is unreachable/rate-limited
    // and the artifact is already known to us.
    const versionListResult = await resolveVersionsList(
      groupId,
      artifactId,
      resolveBlastRadiusMavenBaseUrl(groupId),
    )
    let allVersions: string[]
    if (!('kind' in versionListResult)) {
      allVersions = versionListResult.versions
    } else if (packageId) {
      allVersions = await getVersionNumbers(qx, String(packageId))
    } else {
      throw new Error(
        `Failed to fetch maven-metadata.xml for ${coordinate} (${versionListResult.kind}) and artifact is not in our DB`,
      )
    }

    const vulnerableVersions = versionsInRanges(allVersions, ranges)

    const analyzed = highestVersion(vulnerableVersions)
    if (!analyzed) {
      throw new Error(`Could not determine analyzed version for ${coordinate}`)
    }

    const pkgsrcDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvnsrc-'))
    const patches: Record<string, string> = {}

    try {
      await downloadAndExtractMavenSources(groupId, artifactId, analyzed, pkgsrcDir)

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

      const agentPrompt = buildMavenIntelPrompt(
        osv.id || advisoryOsvId,
        osv.aliases || [],
        osv.details || osv.summary || '',
        analyzed,
        patches,
      )

      const agentResult = await runAnalysisAgent({
        prompt: agentPrompt,
        systemPrompt: MAVEN_INTEL_SYSTEM_PROMPT,
        cwd: pkgsrcDir,
        model: 'claude-opus-4-8',
        schema: MAVEN_INTEL_SCHEMA,
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
        package: coordinate,
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
