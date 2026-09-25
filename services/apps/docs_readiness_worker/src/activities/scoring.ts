import { ApplicationFailure } from '@temporalio/client'

import {
  findProjectForDocsDiscovery,
  replaceProjectDocReadinessChecks,
  upsertProjectDocReadiness,
} from '@crowd/data-access-layer'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'

import { isPrivateOrLoopbackHost } from '../discovery/http'
import { svc } from '../main'
import { loadAfdocs } from '../scoring/afdocs'
import { computeScores } from '../scoring/computeScores'
import { trimReport } from '../scoring/trimReport'
import { IResolvedDocsUrl } from '../types'
import { withTimeout } from './withTimeout'

// afdocs bounds each individual HTTP request (15s default) but not the overall runChecks()
// call; without this, a slow docs site can push the aggregate past Temporal's 30-minute
// activity timeout, and since afdocs exposes no cancellation token, the abandoned call keeps
// running and permanently occupies a worker concurrency slot instead of freeing it on timeout.
const SCORING_TIMEOUT_MS = 25 * 60 * 1000

// Blocks the literal-target vector only; afdocs' own redirect-following fetch isn't intercepted here.
function isUnsafeDocsUrl(raw: string): boolean {
  let url: URL
  try {
    url = new URL(raw)
  } catch {
    return true
  }
  return url.protocol !== 'http:' && url.protocol !== 'https:'
    ? true
    : isPrivateOrLoopbackHost(url.hostname)
}

export async function scoreProject(
  projectId: string,
  runId: string,
  resolved: IResolvedDocsUrl,
): Promise<void> {
  if (!resolved.docsUrl) {
    throw ApplicationFailure.nonRetryable(
      `scoreProject requires a resolved docsUrl for project ${projectId}`,
    )
  }

  if (isUnsafeDocsUrl(resolved.docsUrl)) {
    throw ApplicationFailure.nonRetryable(
      `Refusing to score project ${projectId}: docsUrl resolves to a private or loopback host`,
    )
  }

  const readerQx = pgpQx(svc.postgres.reader.connection())
  const project = await findProjectForDocsDiscovery(readerQx, projectId)
  if (!project) {
    throw ApplicationFailure.nonRetryable(
      `Project ${projectId} not found for docs readiness scoring`,
    )
  }

  const startedAt = Date.now()
  const { runChecks } = await loadAfdocs()
  const report = await withTimeout(
    runChecks(resolved.docsUrl),
    SCORING_TIMEOUT_MS,
    `afdocs runChecks exceeded ${SCORING_TIMEOUT_MS}ms for project ${projectId}`,
  )
  const durationMs = Date.now() - startedAt

  if (report.results.length > 0 && report.results.every((result) => result.status === 'error')) {
    throw ApplicationFailure.create({
      message: `Docs host unreachable while scoring project ${projectId}: every check errored`,
    })
  }

  const { overallScore, overallGrade, categoryScores } = computeScores(report.results)
  const checkRows = trimReport(report.results).map((row) => ({ ...row, durationMs: null }))

  const writerQx = pgpQx(svc.postgres.writer.connection())
  await writerQx.tx(async (tx) => {
    await replaceProjectDocReadinessChecks(tx, projectId, checkRows)
    await upsertProjectDocReadiness(tx, {
      projectId,
      projectSlug: project.slug,
      projectName: project.name,
      docsUrl: resolved.docsUrl,
      discoveryMethod: resolved.discoveryMethod,
      confidence: resolved.confidence,
      isOverride: resolved.isOverride,
      overallScore,
      overallGrade,
      categoryScores,
      runId,
      durationMs,
      ok: true,
      error: null,
    })
  })
}

export async function recordFailure(
  projectId: string,
  runId: string,
  resolved: IResolvedDocsUrl,
  errorMessage: string,
): Promise<void> {
  const readerQx = pgpQx(svc.postgres.reader.connection())
  const project = await findProjectForDocsDiscovery(readerQx, projectId)
  if (!project) {
    throw ApplicationFailure.nonRetryable(
      `Project ${projectId} not found for docs readiness failure record`,
    )
  }

  const writerQx = pgpQx(svc.postgres.writer.connection())
  await writerQx.tx(async (tx) => {
    await replaceProjectDocReadinessChecks(tx, projectId, [])
    await upsertProjectDocReadiness(tx, {
      projectId,
      projectSlug: project.slug,
      projectName: project.name,
      docsUrl: resolved.docsUrl,
      discoveryMethod: resolved.discoveryMethod,
      confidence: resolved.confidence,
      isOverride: resolved.isOverride,
      overallScore: null,
      overallGrade: null,
      categoryScores: null,
      runId,
      durationMs: null,
      ok: false,
      error: errorMessage,
    })
  })
}
