import { ICanonicalRepoUrl, canonicalizeRepoUrl } from '@crowd/common'
import {
  finalizeProjectCatalogEvaluation,
  findGithubOwnersWithLfProjects,
  findGithubOwnersWithNonLfRepos,
  findProjectCatalogById,
  findProjectCatalogPendingEvaluation,
  findRepoUrlsInCdp,
  finishPipelineRun,
  markProjectCatalogPreCheckSkipped,
  promoteProjectsToEvaluate,
  startPipelineRun,
} from '@crowd/data-access-layer'
import { IPipelineRunFinish } from '@crowd/data-access-layer/src/project-catalog-pipeline-runs/types'
import {
  IDbProjectCatalog,
  isGithubDiscussionProvenance,
} from '@crowd/data-access-layer/src/project-catalog/types'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import { SlackChannel, SlackPersona, sendSlackNotificationAsync } from '@crowd/slack'
import { estimateLlmCostUsd } from '@crowd/types'

import { evaluateProject } from '../evaluator/evaluator'
import { svc } from '../main'
import { computeExclusivelyLfOwners, resolvePrecheckSkipReason } from '../precheck/precheck'
import { IEvaluationActivityResult, IPrecheckResult, IPriorityConfig } from '../types'
import { buildSkippedDiscussionAlert } from './skippedRequestAlert'

const log = getServiceLogger()

/**
 * Promotes 'auto' projects to 'evaluate' up to the configured limit.
 * Count, slot computation, locking, and update are all done atomically
 * inside a single SQL statement — see promoteProjectsToEvaluate in the DAL.
 */
export async function promoteProjectsForEvaluation(config: IPriorityConfig): Promise<void> {
  const { evaluateLimit, sourcePriority } = config
  const qx = pgpQx(svc.postgres.writer.connection())

  log.info({ evaluateLimit, sourcePriority }, 'Priority promotion: starting.')

  const promoted = await promoteProjectsToEvaluate(qx, { evaluateLimit, sourcePriority })

  log.info({ promoted }, 'Priority promotion: complete.')
}

export async function fetchPendingProjects(batchSize: number): Promise<IDbProjectCatalog[]> {
  const qx = pgpQx(svc.postgres.reader.connection())

  const projects = await findProjectCatalogPendingEvaluation(qx, { limit: batchSize })

  log.info({ count: projects.length, batchSize }, 'Fetched projects pending evaluation.')

  return projects
}

export async function precheckPendingProjects(
  projects: IDbProjectCatalog[],
): Promise<IPrecheckResult> {
  // Writer connection: avoids replica lag missing a just-written repo/project mapping,
  // same reasoning as evaluateAndUpdateProject's fresh-state read below.
  const writeQx = pgpQx(svc.postgres.writer.connection())

  const canonicalsByProjectId = new Map(
    projects.map((project) => [project.id, canonicalizeRepoUrl(project.repoUrl)] as const),
  )
  const githubCanonicals = [...canonicalsByProjectId.values()].filter(
    (canonical): canonical is ICanonicalRepoUrl & { isGithub: true } =>
      Boolean(canonical?.isGithub),
  )
  const githubCanonicalUrls = githubCanonicals.map((canonical) => canonical.url)
  const githubOwners = githubCanonicals.map((canonical) => canonical.owner)
  const uncanonicalizable = [...canonicalsByProjectId.values()].filter(
    (canonical) => !canonical,
  ).length

  const [reposInCdp, lfOwners, nonLfOwners] = await Promise.all([
    findRepoUrlsInCdp(writeQx, githubCanonicalUrls),
    findGithubOwnersWithLfProjects(writeQx, githubOwners),
    findGithubOwnersWithNonLfRepos(writeQx, githubOwners),
  ])
  const exclusivelyLfOwners = computeExclusivelyLfOwners(lfOwners, nonLfOwners)

  const remaining: IDbProjectCatalog[] = []
  const breakdown: Record<string, number> = {}
  const skippedDiscussionRequests: IPrecheckResult['skippedDiscussionRequests'] = []
  let skippedPreCheck = 0

  const recordSkip = (project: IDbProjectCatalog, reason: string) => {
    skippedPreCheck++
    breakdown[reason] = (breakdown[reason] ?? 0) + 1
    if (isGithubDiscussionProvenance(project.provenance)) {
      skippedDiscussionRequests.push({ project, reason })
    }
  }

  for (const project of projects) {
    const reason = resolvePrecheckSkipReason(canonicalsByProjectId.get(project.id) ?? null, {
      reposInCdp,
      exclusivelyLfOwners,
    })

    if (!reason) {
      remaining.push(project)
      continue
    }

    const updatedRows = await markProjectCatalogPreCheckSkipped(writeQx, project.id, reason)

    if (updatedRows > 0) {
      recordSkip(project, reason)
      continue
    }

    // 0 rows: either a manual request moved this row out of 'evaluate', or a prior
    // attempt of this same (retried) activity already skipped it with this reason.
    const fresh = await findProjectCatalogById(writeQx, project.id)
    const alreadyPrechecked = fresh?.action === 'skip' && fresh?.skipReason === reason

    if (alreadyPrechecked) {
      recordSkip(project, reason)
    } else {
      log.info(
        { id: project.id, repoUrl: project.repoUrl },
        'Project was moved out of evaluate by a manual request while pre-checking, discarding skip.',
      )
    }
  }

  log.info(
    {
      total: projects.length,
      skippedPreCheck,
      remaining: remaining.length,
      breakdown,
      uncanonicalizable,
    },
    'Deterministic pre-check complete.',
  )

  return { remaining, skippedPreCheck, breakdown, skippedDiscussionRequests }
}

export async function evaluateAndUpdateProject(
  project: IDbProjectCatalog,
): Promise<IEvaluationActivityResult | null> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const startTime = Date.now()

  // Guard: fetch fresh state to ensure the API is called at most once per project.
  // Uses the writer connection to avoid replica lag missing a just-written evaluatedAt.
  const fresh = await findProjectCatalogById(qx, project.id)
  if (fresh?.evaluatedAt) {
    log.info(
      { id: project.id, repoUrl: project.repoUrl, evaluatedAt: fresh.evaluatedAt },
      'Project already evaluated, skipping API call.',
    )
    return null
  }

  log.info({ id: project.id, repoUrl: project.repoUrl }, 'Starting evaluation.')

  const result = await evaluateProject({
    id: project.id,
    repoUrl: project.repoUrl,
    repoName: project.repoName,
    projectSlug: project.projectSlug,
    lfCriticalityScore: project.lfCriticalityScore,
    source: project.source,
  })

  const updated = await finalizeProjectCatalogEvaluation(qx, project.id, {
    action: result.outcome,
    evaluationResult: result.evaluationResult,
    evaluationReason: result.evaluationReason,
  })

  const elapsedSeconds = ((Date.now() - startTime) / 1000).toFixed(1)

  if (!updated) {
    log.info(
      { id: project.id, repoUrl: project.repoUrl, elapsedSeconds },
      'Project was moved out of evaluate by a manual request while evaluating, discarding DB update.',
    )
  } else {
    log.info(
      {
        id: project.id,
        repoUrl: project.repoUrl,
        outcome: result.outcome,
        evaluationResult: result.evaluationResult,
        evaluationReason: result.evaluationReason,
        elapsedSeconds,
      },
      'Evaluation complete.',
    )
  }

  if (!result.metrics) {
    return {
      applied: Boolean(updated),
      outcome: result.outcome,
      evaluationReason: result.evaluationReason,
      model: null,
      inputTokens: null,
      outputTokens: null,
      costUsd: null,
      seconds: null,
    }
  }

  const { model, inputTokens, outputTokens, seconds } = result.metrics

  return {
    applied: Boolean(updated),
    outcome: result.outcome,
    evaluationReason: result.evaluationReason,
    model,
    inputTokens,
    outputTokens,
    costUsd: estimateLlmCostUsd(model, inputTokens, outputTokens),
    seconds,
  }
}

export async function notifySkippedHumanRequest(
  project: IDbProjectCatalog,
  reason: string,
): Promise<void> {
  if (!isGithubDiscussionProvenance(project.provenance)) {
    return
  }

  const sent = await sendSlackNotificationAsync(
    SlackChannel.CDP_PROJECT_CATALOG_SKIP_ALERTS,
    SlackPersona.WARNING_PROPAGATOR,
    `Skipped — ${project.repoName}`,
    buildSkippedDiscussionAlert(project, reason),
  )

  if (!sent) {
    log.warn(
      { id: project.id, repoUrl: project.repoUrl },
      'Skipped-discussion Slack alert was not sent.',
    )
  }
}

export async function startEvaluationPipelineRun(
  workflowId: string | null,
  temporalRunId: string | null,
): Promise<string> {
  const qx = pgpQx(svc.postgres.writer.connection())

  const run = await startPipelineRun(qx, { stage: 'evaluation', workflowId, temporalRunId })

  return run.id
}

export async function finishEvaluationPipelineRun(
  id: string,
  data: IPipelineRunFinish,
): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())

  await finishPipelineRun(qx, id, data)
}
