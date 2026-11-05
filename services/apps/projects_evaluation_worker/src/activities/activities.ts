import { Context } from '@temporalio/activity'

import { ONBOARDING_AUTO_APPLY, fetchPublicRepoMetrics, getGithubToken } from '@crowd/common'
import {
  finalizeProjectCatalogEvaluation,
  deferProjectCatalogEvaluation,
  findProjectCatalogById,
  findProjectCatalogPendingEvaluation,
  findProjectCatalogPendingOnboarding,
  findProjectCatalogQuestionsWithoutMessage,
  findProjectCatalogReviewWithoutMessage,
  finishPipelineRun,
  lockProjectCatalogQuestion,
  lockProjectCatalogReviewRequest,
  markProjectCatalogClassified,
  markProjectCatalogEvaluationFailed,
  setProjectCatalogQuestionMessageTs,
  promoteProjectCatalogIdsToEvaluate,
  promoteProjectsToEvaluate,
  setProjectCatalogReviewMessageTs,
  startPipelineRun,
} from '@crowd/data-access-layer'
import { IPipelineRunFinish } from '@crowd/data-access-layer/src/project-catalog-pipeline-runs/types'
import {
  IDbProjectCatalog,
  isGithubDiscussionProvenance,
  isSlackBotProvenance,
} from '@crowd/data-access-layer/src/project-catalog/types'
import { QueryExecutor, pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import {
  BotStatus,
  IOrganizationReview,
  applyRequestStatus,
  buildBotEvaluationErrorContent,
  buildBotQuestionContent,
  buildBotReviewReply,
  buildBotSkippedReply,
  classifyCatalogRow,
  findOrganizationSiblings,
  organizationOf,
  organizationReviewOf,
  refreshRequestStatusMessage,
  reportRequestOutcome,
  resolveReviewCdpUrl,
  routeEvaluationOutcome,
  sameQuestion,
  sameReviewProposal,
  toReviewRepoContext,
  toReviewRequestCard,
  organizationGroupKey,
} from '@crowd/project-onboarding'
import {
  loadBatchRepoLookups,
  withCatalogRowClassifierDeps,
} from '@crowd/project-onboarding/src/requestClassifierDeps'
import {
  ISlackMessageContent,
  IReviewRepoContext,
  SlackChannel,
  SlackPersona,
  postReviewRequest,
  postSlackContentInThread,
  sendSlackNotificationAsync,
} from '@crowd/slack'
import { WorkflowIdReusePolicy } from '@crowd/temporal'
import { estimateLlmCostUsd } from '@crowd/types'

import { evaluateProject } from '../evaluator/evaluator'
import { MAX_EVALUATION_RUNS } from '../evaluator/retry'
import { IEvaluationResult } from '../evaluator/types'
import { svc } from '../main'
import {
  IClassifyAndRouteResult,
  IEvaluationActivityResult,
  IImmediateOnboardingPlan,
  IImmediateOnboardingResult,
  IPriorityConfig,
} from '../types'
import { classifyAndRouteProjects } from './classifyAndRoute'
import { canDeferEvaluation } from './evaluationRetry'
import { buildImmediateOnboardingPlan } from './immediateOnboarding'
import {
  ISharedEvaluation,
  sharedEvaluationOutcome,
  sharedOrganizationEvaluation,
} from './organizationEvaluation'
import { buildRequestAlert, buildSkippedDiscussionAlert } from './skippedRequestAlert'

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

export async function promoteCatalogIdsForEvaluation(catalogIds: string[]): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())

  const promoted = await promoteProjectCatalogIdsToEvaluate(qx, catalogIds)

  log.info({ catalogIds, promoted }, 'Promoted the requested catalog rows to evaluate.')
}

export async function fetchPendingProjects(
  batchSize: number,
  catalogIds?: string[],
): Promise<IDbProjectCatalog[]> {
  // Requested ids were promoted on the writer moments ago; the replica may not have them yet.
  const pool = catalogIds ? svc.postgres.writer : svc.postgres.reader
  const qx = pgpQx(pool.connection())

  const batch = await findProjectCatalogPendingEvaluation(qx, {
    limit: catalogIds ? undefined : batchSize,
    ids: catalogIds,
  })
  const projects = await withWholeOrganizations(qx, batch)

  log.info(
    { count: projects.length, batchSize, catalogIds },
    'Fetched projects pending evaluation.',
  )

  return projects
}

function awaitsEvaluation(row: IDbProjectCatalog): boolean {
  return row.action === 'evaluate' || row.action === 'auto'
}

// A row limit or a capped promotion must not split an organization across runs, or it gets
// two questions or cards.
async function withWholeOrganizations(
  qx: QueryExecutor,
  batch: IDbProjectCatalog[],
): Promise<IDbProjectCatalog[]> {
  const included = new Set(batch.map((row) => row.id))
  const leads = new Map<string, IDbProjectCatalog>()
  for (const row of batch) {
    const key = organizationGroupKey(row)
    if (key && !leads.has(key)) {
      leads.set(key, row)
    }
  }
  const completed = [...batch]
  const notPromoted: string[] = []
  for (const lead of leads.values()) {
    const siblings = await findOrganizationSiblings(
      qx,
      lead,
      (row) => awaitsEvaluation(row) && !included.has(row.id),
    )
    for (const sibling of siblings) {
      included.add(sibling.id)
      completed.push(sibling)
      if (sibling.action === 'auto') {
        notPromoted.push(sibling.id)
      }
    }
  }
  await promoteProjectCatalogIdsToEvaluate(pgpQx(svc.postgres.writer.connection()), notPromoted)
  return completed
}

export async function classifyAndRoutePendingProjects(
  projects: IDbProjectCatalog[],
): Promise<IClassifyAndRouteResult> {
  // Writer connection: avoids replica lag missing a just-written repo/project mapping,
  // same reasoning as evaluateAndUpdateProject's fresh-state read below.
  const writeQx = pgpQx(svc.postgres.writer.connection())

  const repoLookups = await loadBatchRepoLookups(
    writeQx,
    projects.map((project) => project.repoUrl),
  )

  const result = await withCatalogRowClassifierDeps(
    writeQx,
    (deps) =>
      classifyAndRouteProjects(projects, {
        findFresh: (id) => findProjectCatalogById(writeQx, id),
        classify: async (row) => (await classifyCatalogRow(row, deps)).classification,
        markClassified: (id, classification) =>
          markProjectCatalogClassified(writeQx, id, classification),
        markFailed: (id, reason) => markProjectCatalogEvaluationFailed(writeQx, id, reason),
        findTrackedRepoUrls: (repoUrls) => repoLookups.findTrackedRepoUrls(repoUrls),
        onRowStart: (project) =>
          Context.current().heartbeat({ stage: 'classification', id: project.id }),
      }),
    repoLookups,
  )

  log.info(
    {
      total: projects.length,
      routed: result.routed,
      breakdown: result.breakdown,
      discarded: result.discarded,
      remaining: result.remaining.length,
    },
    'Classification and routing complete.',
  )

  return result
}

function isRecordedCheckError(row: IDbProjectCatalog): boolean {
  return row.action === 'error' && row.evaluationResult === 'error' && row.onboardingError === null
}

// A retried attempt must still return the error the dead attempt stored, or the workflow never
// posts the thread notice and the alert for it.
function recordedErrorResult(row: IDbProjectCatalog): IEvaluationActivityResult {
  return {
    applied: true,
    outcome: 'unsure',
    action: 'error',
    evaluationReason: row.evaluationReason,
    model: null,
    inputTokens: null,
    outputTokens: null,
    costUsd: null,
    seconds: null,
  }
}

interface IEvaluationGroup {
  organization: IOrganizationReview | null
  siblings: IDbProjectCatalog[]
}

const NO_EVALUATOR_USAGE = {
  model: null,
  inputTokens: null,
  outputTokens: null,
  costUsd: null,
  seconds: null,
}

async function findEvaluationGroup(
  qx: QueryExecutor,
  project: IDbProjectCatalog,
): Promise<IEvaluationGroup> {
  if (!organizationGroupKey(project)) {
    return { organization: null, siblings: [] }
  }
  const siblings = await findOrganizationSiblings(qx, project, (row) =>
    sameReviewProposal(row, project),
  )
  return { organization: organizationReviewOf(project, siblings.length), siblings }
}

type EvaluatorUsage = Pick<
  IEvaluationActivityResult,
  'model' | 'inputTokens' | 'outputTokens' | 'costUsd' | 'seconds'
>

function evaluatorUsageOf(result: IEvaluationResult): EvaluatorUsage {
  if (!result.metrics) {
    return NO_EVALUATOR_USAGE
  }
  const { model, inputTokens, outputTokens, seconds } = result.metrics
  return {
    model,
    inputTokens,
    outputTokens,
    costUsd: estimateLlmCostUsd(model, inputTokens, outputTokens),
    seconds,
  }
}

// The skip reply of the organization went out with the first row; the status line and the
// reactions still have to see this row settle.
async function refreshSkippedRequestStatus(
  qx: QueryExecutor,
  project: IDbProjectCatalog,
): Promise<void> {
  try {
    await refreshRequestStatusMessage(qx, project)
    await applyRequestStatus(qx, project, skippedStatus(project))
  } catch (err) {
    log.warn(
      { id: project.id, repoUrl: project.repoUrl, err },
      'Request status was not refreshed after a shared skip.',
    )
  }
}

// One evaluator call per organization: the rows after the first take its verdict.
async function applySharedEvaluation(
  qx: QueryExecutor,
  project: IDbProjectCatalog,
  shared: ISharedEvaluation,
  organization: IOrganizationReview | null,
  usage: EvaluatorUsage = NO_EVALUATOR_USAGE,
): Promise<IEvaluationActivityResult> {
  const updated = await finalizeProjectCatalogEvaluation(qx, project.id, shared)
  if (updated && shared.action === 'skip' && isSlackBotProvenance(project.provenance)) {
    await refreshSkippedRequestStatus(qx, project)
  }
  log.info(
    {
      id: project.id,
      repoUrl: project.repoUrl,
      organization: organization?.owner,
      action: shared.action,
      evaluationResult: shared.evaluationResult,
      applied: Boolean(updated),
    },
    'Evaluation taken from the organization.',
  )
  return {
    applied: Boolean(updated),
    outcome: sharedEvaluationOutcome(shared),
    action: shared.action,
    evaluationReason: shared.evaluationReason,
    ...usage,
    shared: true,
    organization,
  }
}

export async function evaluateAndUpdateProject(
  project: IDbProjectCatalog,
): Promise<IEvaluationActivityResult | null> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const startTime = Date.now()

  // Guard: fetch fresh state to ensure the API is called at most once per project.
  // Uses the writer connection to avoid replica lag missing a just-written evaluatedAt.
  const fresh = await findProjectCatalogById(qx, project.id)
  if (fresh && isRecordedCheckError(fresh)) {
    log.info(
      { id: project.id, repoUrl: project.repoUrl },
      'Check error recorded by an earlier attempt.',
    )
    return recordedErrorResult(fresh)
  }
  if (fresh?.evaluatedAt) {
    log.info(
      { id: project.id, repoUrl: project.repoUrl, evaluatedAt: fresh.evaluatedAt },
      'Project already evaluated, skipping API call.',
    )
    return null
  }

  const { organization, siblings } = await findEvaluationGroup(qx, project)
  const shared = sharedOrganizationEvaluation(project, siblings)
  if (shared) {
    return applySharedEvaluation(qx, project, shared, organization)
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

  // Two workflows can evaluate siblings of one organization at once; the verdict written first
  // wins, so the organization never carries two.
  const late =
    siblings.length > 0
      ? sharedOrganizationEvaluation(project, (await findEvaluationGroup(qx, project)).siblings)
      : null
  if (late) {
    log.info(
      { id: project.id, repoUrl: project.repoUrl },
      'A sibling was evaluated meanwhile, taking its verdict.',
    )
    return applySharedEvaluation(qx, project, late, organization, evaluatorUsageOf(result))
  }

  const route = routeEvaluationOutcome(project, result, {
    autoApply: ONBOARDING_AUTO_APPLY,
    organization,
  })
  const updated = await finalizeProjectCatalogEvaluation(qx, project.id, {
    action: route.action,
    evaluationResult: result.evaluationResult,
    evaluationReason: result.evaluationReason,
    reviewReason: route.reviewReason,
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
        action: route.action,
        evaluationResult: result.evaluationResult,
        evaluationReason: result.evaluationReason,
        elapsedSeconds,
      },
      'Evaluation complete.',
    )
  }

  return {
    applied: Boolean(updated),
    outcome: result.outcome,
    action: route.action,
    evaluationReason: result.evaluationReason,
    organization,
    ...evaluatorUsageOf(result),
  }
}

async function replyInBotThread(
  project: Pick<IDbProjectCatalog, 'id' | 'repoUrl' | 'sourceUrl' | 'requestHints'>,
  content: string | ISlackMessageContent,
  status: BotStatus,
): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const result = await reportRequestOutcome(qx, project, content)

  if (!result.ok) {
    log.warn(
      {
        id: project.id,
        repoUrl: project.repoUrl,
        sourceUrl: project.sourceUrl,
        error: result.error,
      },
      `Slack bot thread reply (${status}) was not sent.`,
    )
  }

  await applyRequestStatus(pgpQx(svc.postgres.writer.connection()), project, status)
}

function skippedStatus(project: Pick<IDbProjectCatalog, 'classificationKind'>): BotStatus {
  return project.classificationKind === 'already_tracked' ? 'alreadyThere' : 'notAdded'
}

export async function notifySkippedHumanRequest(
  project: IDbProjectCatalog,
  reason: string,
  organization: IOrganizationReview | null = null,
): Promise<void> {
  if (isSlackBotProvenance(project.provenance)) {
    await replyInBotThread(
      project,
      buildBotSkippedReply(project, reason, organization),
      skippedStatus(project),
    )
    return
  }

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

const QUESTION_LIMIT = 50

export async function fetchPendingQuestions(catalogIds?: string[]): Promise<IDbProjectCatalog[]> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const projects = await findProjectCatalogQuestionsWithoutMessage(qx, {
    limit: catalogIds ? undefined : QUESTION_LIMIT,
    ids: catalogIds,
  })
  log.info({ count: projects.length, catalogIds }, 'Fetched questions not yet posted.')
  return projects
}

// A scheduled sweep may reach a group while another run still classifies it; the card or the
// question waits for the whole organization, or part of it would get a message of its own.
async function findSiblingsOnceClassified(
  qx: QueryExecutor,
  lead: IDbProjectCatalog,
  keep: (row: IDbProjectCatalog) => boolean,
): Promise<IDbProjectCatalog[] | null> {
  const siblings = await findOrganizationSiblings(qx, lead, () => true)
  const classifying = siblings.filter(awaitsEvaluation)
  if (classifying.length === 0) {
    return siblings.filter(keep)
  }
  const notPromoted = classifying.filter((row) => row.action === 'auto').map((row) => row.id)
  await promoteProjectCatalogIdsToEvaluate(qx, notPromoted)
  log.info(
    {
      id: lead.id,
      organization: organizationOf(lead),
      classifying: classifying.length,
      promoted: notPromoted.length,
    },
    'Organization still being classified, the message waits for the next run.',
  )
  return null
}

function awaitsSameQuestion(lead: IDbProjectCatalog): (row: IDbProjectCatalog) => boolean {
  return (row) =>
    row.action === 'ask' &&
    !row.requestQuestion?.messageTs &&
    sameQuestion(row.requestQuestion, lead.requestQuestion)
}

// Rows are locked in id order so two runs sharing a group never wait on each other in a cycle.
async function lockGroup(
  rows: IDbProjectCatalog[],
  lock: (id: string) => Promise<boolean>,
): Promise<Set<string>> {
  const locked = new Set<string>()
  for (const row of [...rows].sort((a, b) => a.id.localeCompare(b.id))) {
    if (await lock(row.id)) {
      locked.add(row.id)
    }
  }
  return locked
}

async function hasPendingQuestion(qx: QueryExecutor, id: string): Promise<boolean> {
  const fresh = await findProjectCatalogById(qx, id)
  return fresh?.action === 'ask' && !!fresh.requestQuestion && !fresh.requestQuestion.messageTs
}

// The question is a new message on purpose: the requester must act. The row lock keeps two
// runs from asking twice; the status line and the ❓ reaction follow a successful post.
export async function askRequester(project: IDbProjectCatalog): Promise<void> {
  if (!project.requestQuestion) {
    return
  }
  const qx = pgpQx(svc.postgres.writer.connection())
  if (!(await hasPendingQuestion(qx, project.id))) {
    log.info(
      { id: project.id, repoUrl: project.repoUrl },
      'Question already posted or row moved on.',
    )
    return
  }
  const candidates = await findSiblingsOnceClassified(qx, project, awaitsSameQuestion(project))
  if (!candidates) {
    return
  }

  const posted = await qx.tx(async (tx): Promise<boolean> => {
    const locked = await lockGroup([project, ...candidates], (id) =>
      lockProjectCatalogQuestion(tx, id),
    )
    if (!locked.has(project.id)) {
      log.info(
        { id: project.id, repoUrl: project.repoUrl },
        'Question already posted or row moved on.',
      )
      return false
    }
    const siblings = candidates.filter((row) => locked.has(row.id))
    const content = buildBotQuestionContent(project, organizationReviewOf(project, siblings.length))
    if (!content) {
      return false
    }
    const result = await postSlackContentInThread(project.sourceUrl, content)
    if (!result.ok || !result.ts) {
      log.warn(
        { id: project.id, repoUrl: project.repoUrl, error: result.error },
        'Question for the requester was not posted.',
      )
      return false
    }
    for (const row of [project, ...siblings]) {
      await setProjectCatalogQuestionMessageTs(tx, row.id, result.ts)
    }
    return true
  })

  if (posted) {
    await refreshRequestStatusMessage(qx, project)
    await applyRequestStatus(qx, project, 'needsInput')
  }
}

export type EvaluationFailureRecord = 'failed' | 'deferred' | 'moved'

type FailedEvaluationRow = Pick<IDbProjectCatalog, 'id' | 'provenance' | 'evaluationAttempts'>

async function isAlreadyDeferred(
  qx: QueryExecutor,
  project: FailedEvaluationRow,
): Promise<boolean> {
  const current = await findProjectCatalogById(qx, project.id)
  return (
    current?.action === 'evaluate' &&
    current.evaluatedAt === null &&
    current.evaluationAttempts === project.evaluationAttempts + 1
  )
}

async function deferEvaluation(
  qx: QueryExecutor,
  project: FailedEvaluationRow,
  reason: string,
): Promise<boolean> {
  if (!canDeferEvaluation(project)) {
    return false
  }
  const deferred = await deferProjectCatalogEvaluation(qx, project.id, {
    reason,
    expectedAttempts: project.evaluationAttempts,
    maxRuns: MAX_EVALUATION_RUNS,
  })
  if (!deferred) {
    return isAlreadyDeferred(qx, project)
  }
  log.warn(
    { id: project.id, reason, attempts: deferred.evaluationAttempts },
    'Evaluation hit a transient error, the row waits for the next run.',
  )
  return true
}

export async function markEvaluationFailed(
  project: FailedEvaluationRow,
  reason: string,
  transient = false,
): Promise<EvaluationFailureRecord> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const projectId = project.id
  if (transient && (await deferEvaluation(qx, project, reason))) {
    return 'deferred'
  }
  const marked = await markProjectCatalogEvaluationFailed(
    qx,
    projectId,
    reason,
    transient ? project.evaluationAttempts : undefined,
  )
  if (marked > 0) {
    log.error({ id: projectId, reason }, 'Evaluation failed, marked as error.')
    return 'failed'
  }
  const current = await findProjectCatalogById(qx, projectId)
  if (current && isRecordedCheckError(current)) {
    log.info({ id: projectId }, 'Check error already recorded by an earlier attempt.')
    return 'failed'
  }
  log.info({ id: projectId }, 'Row left evaluate before the failure could be recorded.')
  return 'moved'
}

// The requester must act, so the Try again notice is always a new message; the status
// message is refreshed as well when the request has one.
async function postTryAgainInBotThread(project: IDbProjectCatalog): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const posted = await postSlackContentInThread(
    project.sourceUrl,
    buildBotEvaluationErrorContent(project),
  )
  if (!posted.ok) {
    log.warn(
      { id: project.id, repoUrl: project.repoUrl, error: posted.error },
      'Try again notice was not posted in the Slack thread.',
    )
  }
  await refreshRequestStatusMessage(qx, project)
  await applyRequestStatus(qx, project, 'problem')
}

export async function notifyEvaluationError(
  project: IDbProjectCatalog,
  reason: string,
): Promise<void> {
  if (isSlackBotProvenance(project.provenance)) {
    await postTryAgainInBotThread(project)
  } else if (!isGithubDiscussionProvenance(project.provenance)) {
    return
  }

  const sent = await sendSlackNotificationAsync(
    SlackChannel.CDP_PROJECT_CATALOG_SKIP_ALERTS,
    SlackPersona.ERROR_REPORTER,
    `Check failed — ${project.repoName}`,
    buildRequestAlert(project, 'Error', reason),
  )
  if (!sent) {
    log.warn({ id: project.id, repoUrl: project.repoUrl }, 'Evaluation error alert was not sent.')
  }
}

const REVIEW_REQUEST_LIMIT = 50

export async function fetchReviewRequests(catalogIds?: string[]): Promise<IDbProjectCatalog[]> {
  const qx = pgpQx(svc.postgres.writer.connection())

  const projects = await findProjectCatalogReviewWithoutMessage(qx, {
    limit: catalogIds ? undefined : REVIEW_REQUEST_LIMIT,
    ids: catalogIds,
  })

  log.info({ count: projects.length, catalogIds }, 'Fetched review rows without a Slack request.')

  return projects
}

function awaitsSameReviewCard(lead: IDbProjectCatalog): (row: IDbProjectCatalog) => boolean {
  return (row) => row.action === 'review' && !row.reviewMessageTs && sameReviewProposal(row, lead)
}

interface IPostedReviewRequest {
  organization: IOrganizationReview | null
}

// The lock below is authoritative; this read spares the siblings of a posted organization card
// the repo metrics fetch, which they would otherwise pay before the lock turns them away.
async function hasPendingReviewCard(qx: QueryExecutor, id: string): Promise<boolean> {
  const fresh = await findProjectCatalogById(qx, id)
  return fresh?.action === 'review' && !fresh.reviewMessageTs
}

export async function notifyReviewRequest(project: IDbProjectCatalog): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())
  if (!(await hasPendingReviewCard(qx, project.id))) {
    log.info(
      { id: project.id, repoUrl: project.repoUrl },
      'Review request already posted or the row left review.',
    )
    return
  }
  const candidates = await findSiblingsOnceClassified(qx, project, awaitsSameReviewCard(project))
  if (!candidates) {
    return
  }
  const cdpUrl = await resolveReviewCdpUrl(qx, project, process.env.CROWD_API_FRONTEND_URL)
  const preloadedRepoContext =
    candidates.length > 0 ? undefined : await loadReviewRepoContext(project)

  const posted = await qx.tx(async (tx): Promise<IPostedReviewRequest | null> => {
    const locked = await lockGroup([project, ...candidates], (id) =>
      lockProjectCatalogReviewRequest(tx, id),
    )
    if (!locked.has(project.id)) {
      log.info(
        { id: project.id, repoUrl: project.repoUrl },
        'Review request already posted by another run or the row left review.',
      )
      return null
    }
    const siblings = candidates.filter((row) => locked.has(row.id))
    const organization = organizationReviewOf(project, siblings.length)
    const repoContext = organization ? null : await singleRepoContext(project, preloadedRepoContext)
    const extras = { cdpUrl, repoContext }

    const result = await postReviewRequest(
      toReviewRequestCard(project, extras, organization),
      project.sourceUrl,
    )
    if (!result.ok || !result.ts) {
      log.warn(
        { id: project.id, repoUrl: project.repoUrl, error: result.error },
        'Review request Slack message was not sent.',
      )
      return null
    }

    for (const row of [project, ...siblings]) {
      await setProjectCatalogReviewMessageTs(tx, row.id, result.ts)
    }
    log.info(
      { id: project.id, repoUrl: project.repoUrl, ts: result.ts, siblings: siblings.length },
      'Review request posted.',
    )
    return { organization }
  })

  if (posted && isSlackBotProvenance(project.provenance)) {
    await replyInBotThread(project, buildBotReviewReply(project, posted.organization), 'withTeam')
  }
}

// Well under the notify activity's one-minute, no-retry deadline so the card is still posted.
const REPO_CONTEXT_TIMEOUT_MS = 10_000

async function singleRepoContext(
  project: Pick<IDbProjectCatalog, 'id' | 'repoUrl'>,
  preloaded: IReviewRepoContext | null | undefined,
): Promise<IReviewRepoContext | null> {
  return preloaded === undefined ? loadReviewRepoContext(project) : preloaded
}

async function loadReviewRepoContext(
  project: Pick<IDbProjectCatalog, 'id' | 'repoUrl'>,
): Promise<IReviewRepoContext | null> {
  try {
    const metrics = await fetchPublicRepoMetrics(
      project.repoUrl,
      getGithubToken(),
      AbortSignal.timeout(REPO_CONTEXT_TIMEOUT_MS),
    )
    return toReviewRepoContext(metrics)
  } catch (err) {
    log.warn(
      { id: project.id, repoUrl: project.repoUrl, err },
      'GitHub context for the review card was not loaded.',
    )
    return null
  }
}

async function startImmediateOnboarding(catalogIds: string[], workflowId: string): Promise<void> {
  try {
    await svc.temporal.workflow.start('onboardProjects', {
      taskQueue: 'automatic-onboarding',
      workflowId: `onboard-${workflowId}`,
      workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
      args: [{ catalogIds }],
    })
  } catch (err) {
    if (err instanceof Error && err.name === 'WorkflowExecutionAlreadyStartedError') {
      log.info({ workflowId, catalogIds }, 'Immediate onboarding already started.')
      return
    }
    throw err
  }
}

export async function planImmediateOnboarding(
  catalogIds: string[],
): Promise<IImmediateOnboardingPlan> {
  const qx = pgpQx(svc.postgres.writer.connection())

  const pendingOnboarding = await findProjectCatalogPendingOnboarding(qx, { ids: catalogIds })

  return buildImmediateOnboardingPlan(pendingOnboarding, ONBOARDING_AUTO_APPLY)
}

export async function applyImmediateOnboarding(
  plan: IImmediateOnboardingPlan,
  workflowId: string,
): Promise<IImmediateOnboardingResult> {
  if (plan.toOnboard.length === 0) {
    return { onboarding: [] }
  }

  await startImmediateOnboarding(
    plan.toOnboard.map((row) => row.id),
    workflowId,
  )
  const onboarding = plan.toOnboard.map((row) => row.repoUrl)
  log.info({ repoUrls: onboarding }, 'Immediate onboarding started for the slack bot request.')

  return { onboarding }
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
