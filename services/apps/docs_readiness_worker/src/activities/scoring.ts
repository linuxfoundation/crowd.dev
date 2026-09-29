import { ApplicationFailure } from '@temporalio/client'

import {
  REPO_ONLY_ERROR,
  findLatestProjectDocReadiness,
  findProjectForDocsDiscovery,
  lockProjectDocReadiness,
  replaceProjectDocReadinessChecks,
  upsertProjectDocReadiness,
} from '@crowd/data-access-layer'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'

import { isPrivateOrLoopbackHost, normalizeUrl } from '../discovery/http'
import { svc } from '../main'
import { computeScores } from '../scoring/computeScores'
import { DeadlineExceededError } from '../scoring/deadlineError'
import { runChecksIsolated } from '../scoring/runChecksIsolated'
import { trimReport } from '../scoring/trimReport'
import { IResolvedDocsUrl } from '../types'

// afdocs bounds each HTTP request but not the whole run, and parses pages synchronously; the
// worker thread it runs in is terminated at this deadline, so a huge docs site cannot block us.
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

  // afdocs resolves github.com/llms.txt for a repo page and would score GitHub's own files.
  if (resolved.discoveryMethod === 'repo-url') {
    throw ApplicationFailure.nonRetryable(REPO_ONLY_ERROR)
  }

  const readerQx = pgpQx(svc.postgres.reader.connection())
  const project = await findProjectForDocsDiscovery(readerQx, projectId)
  if (!project) {
    throw ApplicationFailure.nonRetryable(
      `Project ${projectId} not found for docs readiness scoring`,
    )
  }

  const startedAt = Date.now()
  const report = await runChecksIsolated(
    resolved.docsUrl,
    SCORING_TIMEOUT_MS,
    `afdocs runChecks exceeded ${SCORING_TIMEOUT_MS}ms for project ${projectId}`,
  ).catch((err) => {
    // A retry would burn another full deadline on the same oversized site.
    throw err instanceof DeadlineExceededError ? ApplicationFailure.nonRetryable(err.message) : err
  })
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
    await lockProjectDocReadiness(tx, projectId)
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
    await lockProjectDocReadiness(tx, projectId)
    // Check rows always describe the latest row's URL, so keep them only when this run has the same URL.
    const latest = await findLatestProjectDocReadiness(tx, projectId)
    const failedUrl = resolved.docsUrl ? normalizeUrl(resolved.docsUrl) : null
    const latestUrl = latest?.docsUrl ? normalizeUrl(latest.docsUrl) : null
    if (!failedUrl || failedUrl !== latestUrl) {
      await replaceProjectDocReadinessChecks(tx, projectId, [])
    }
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
