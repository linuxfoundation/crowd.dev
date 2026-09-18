import { ApplicationFailure } from '@temporalio/client'

import {
  findProjectForDocsDiscovery,
  replaceProjectDocReadinessChecks,
  upsertProjectDocReadiness,
} from '@crowd/data-access-layer'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'

import { svc } from '../main'
import { loadAfdocs } from '../scoring/afdocs'
import { computeScores } from '../scoring/computeScores'
import { trimReport } from '../scoring/trimReport'
import { IResolvedDocsUrl } from '../types'

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

  const readerQx = pgpQx(svc.postgres.reader.connection())
  const project = await findProjectForDocsDiscovery(readerQx, projectId)
  if (!project) {
    throw ApplicationFailure.nonRetryable(
      `Project ${projectId} not found for docs readiness scoring`,
    )
  }

  const startedAt = Date.now()
  const { runChecks } = await loadAfdocs()
  const report = await runChecks(resolved.docsUrl)
  const durationMs = Date.now() - startedAt

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
  await upsertProjectDocReadiness(writerQx, {
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
}
