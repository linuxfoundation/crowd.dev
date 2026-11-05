import {
  claimProjectCatalogForOnboarding,
  findProjectCatalogById,
  isProjectCatalogOnboardingPending,
  lockProjectCatalogOnboardingTarget,
  findProjectCatalogPendingOnboarding,
  finishPipelineRun,
  markProjectCatalogOnboardingFailed,
  markProjectCatalogOnboardingSkipped,
  releaseProjectCatalogOnboardingClaim,
  startPipelineRun,
  updateProjectCatalog,
} from '@crowd/data-access-layer'
import {
  InsightsProjectField,
  findInsightsProjectBySlugIncludingDeleted,
} from '@crowd/data-access-layer/src/collections'
import { IPipelineRunFinish } from '@crowd/data-access-layer/src/project-catalog-pipeline-runs/types'
import {
  IDbProjectCatalog,
  isSlackBotProvenance,
} from '@crowd/data-access-layer/src/project-catalog/types'
import { QueryExecutor, pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import {
  BotStatus,
  IOnboardingResult,
  NO_REVIEW_CARD_EXTRAS,
  applyRequestStatus,
  buildBotErroredReply,
  buildBotOnboardedContent,
  buildBotSkippedReply,
  connectReposToSegment,
  createsNewProject,
  deriveProjectSlug,
  findOnboardedInsightsProject,
  findOrganizationSiblings,
  onboardOrganizationProject,
  onboardProject,
  organizationGroupKey,
  organizationOf,
  organizationReviewOf,
  reportRequestOutcome,
  toReviewRequestCard,
} from '@crowd/project-onboarding'
import { createCdpSegmentLookup } from '@crowd/project-onboarding/src/cdpSegmentLookup'
import {
  ISlackMessageContent,
  SlackChannel,
  SlackPersona,
  buildReviewClosedMessage,
  getSlackBotConfig,
  parseSlackPermalink,
  sendSlackNotificationAsync,
  updateSlackMessage,
} from '@crowd/slack'

import { svc } from '../main'
import {
  IDeletedProjectSkips,
  IOrganizationOnboardingOutcome,
  OnboardAndUpdateProjectOutcome,
} from '../types'
import {
  buildErroredDiscussionAlert,
  buildOnboardedDiscussionAlert,
  buildOnboardedDiscussionTitle,
  groupGithubDiscussionRequestsBySource,
  isGithubDiscussionRequest,
} from './discussionRequestAlert'
import { buildInsightsProjectSkipReason, needsDeletedProjectCheck } from './insightsProjectSkip'
import {
  runOrganizationProposedActionSerialized,
  runProposedActionSerialized,
} from './proposedAction'

const log = getServiceLogger()

export async function fetchProjectsPendingOnboarding(
  batchSize: number,
  catalogIds?: string[],
): Promise<IDbProjectCatalog[]> {
  // Requested ids were routed to onboard on the writer moments ago; the replica may not have them yet.
  const pool = catalogIds ? svc.postgres.writer : svc.postgres.reader
  const qx = pgpQx(pool.connection())

  const batch = await findProjectCatalogPendingOnboarding(qx, {
    limit: catalogIds ? undefined : batchSize,
    ids: catalogIds,
  })
  const projects = catalogIds ? batch : await withWholeOrganizations(qx, batch)

  log.info(
    { count: projects.length, batchSize, catalogIds },
    'Fetched projects pending onboarding.',
  )

  return projects
}

// The row limit must not split an organization across runs, or it gets several integration calls.
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
  for (const lead of leads.values()) {
    const siblings = await findOrganizationSiblings(
      qx,
      lead,
      (row) => isProjectCatalogOnboardingPending(row) && !included.has(row.id),
    )
    for (const sibling of siblings) {
      included.add(sibling.id)
      completed.push(sibling)
    }
  }
  return completed
}

async function resolveUnclaimedOutcome(
  qx: ReturnType<typeof pgpQx>,
  project: IDbProjectCatalog,
): Promise<OnboardAndUpdateProjectOutcome> {
  const current = await findProjectCatalogById(qx, project.id)
  if (current?.onboardedAt) {
    log.info(
      { id: project.id, repoUrl: project.repoUrl, onboardedAt: current.onboardedAt },
      'Project already onboarded or claimed by another run, skipping API call.',
    )
    return 'already-onboarded'
  }
  if (current?.action === 'skip') {
    log.info({ id: project.id, repoUrl: project.repoUrl }, 'Project already skipped.')
    return 'already-skipped'
  }
  log.info(
    { id: project.id, repoUrl: project.repoUrl, action: current?.action },
    'Project catalog row changed concurrently, not calling onboarding API.',
  )
  return 'catalog-changed'
}

async function findDeletedInsightsProjectBySlug(qx: ReturnType<typeof pgpQx>, projectSlug: string) {
  const slug = deriveProjectSlug(projectSlug)
  const insightsProject = await findInsightsProjectBySlugIncludingDeleted(qx, slug, [
    InsightsProjectField.DELETED_AT,
  ])
  return insightsProject?.deletedAt ? insightsProject : null
}

function buildProposedActionDeps(depsQx: QueryExecutor) {
  return {
    findCdpSegmentByPccProject: createCdpSegmentLookup(depsQx),
    onboardProject,
    onboardOrganizationProject,
    connectReposToSegment,
  }
}

async function skipIfInsightsProjectDeleted(
  qx: QueryExecutor,
  project: IDbProjectCatalog,
): Promise<'skipped' | 'raced' | 'not-deleted'> {
  if (!needsDeletedProjectCheck(project)) {
    return 'not-deleted'
  }
  const deletedInsightsProject = await findDeletedInsightsProjectBySlug(qx, project.projectSlug)
  if (!deletedInsightsProject) {
    return 'not-deleted'
  }
  const reason = buildInsightsProjectSkipReason(deletedInsightsProject.deletedAt)
  const updatedRows = await markProjectCatalogOnboardingSkipped(qx, project.id, reason)
  if (updatedRows === 0) {
    return 'raced'
  }
  log.info({ id: project.id, repoUrl: project.repoUrl, reason }, 'Onboarding skipped.')
  return 'skipped'
}

// The scheduled run and a bot-started run can hold the same row: stamping onboardedAt
// atomically before the external call lets exactly one proceed; dead claims expire in the DAL.
async function claimForOnboarding(
  qx: QueryExecutor,
  project: IDbProjectCatalog,
): Promise<'claimed' | OnboardAndUpdateProjectOutcome> {
  const skip = await skipIfInsightsProjectDeleted(qx, project)
  if (skip === 'skipped') {
    return 'skipped'
  }
  if (skip === 'raced') {
    return resolveUnclaimedOutcome(qx, project)
  }
  const claimed = await claimProjectCatalogForOnboarding(qx, project.id)
  return claimed ? 'claimed' : resolveUnclaimedOutcome(qx, project)
}

async function markOnboarded(
  qx: QueryExecutor,
  project: IDbProjectCatalog,
  segmentId: string | null,
): Promise<void> {
  await updateProjectCatalog(qx, project.id, {
    action: 'onboarded',
    onboardedAt: new Date().toISOString(),
    onboardingError: null,
    matchedSegmentId: segmentId ?? project.matchedSegmentId,
  })
}

// Skips are recorded before any claim, so a failed group call cannot swallow their replies.
export async function skipRowsOfDeletedProjects(
  projects: IDbProjectCatalog[],
): Promise<IDeletedProjectSkips> {
  const qx = pgpQx(svc.postgres.writer.connection())
  return qx.tx(async (tx) => {
    const result: IDeletedProjectSkips = { skipped: [], remaining: [] }
    for (const project of projects) {
      const skip = await skipIfInsightsProjectDeleted(tx, project)
      if (skip === 'skipped') {
        result.skipped.push(project)
      } else {
        result.remaining.push(project)
      }
    }
    return result
  })
}

async function claimOrganizationRows(
  qx: QueryExecutor,
  projects: IDbProjectCatalog[],
  outcome: IOrganizationOnboardingOutcome,
): Promise<IDbProjectCatalog[]> {
  const claimed: IDbProjectCatalog[] = []
  try {
    for (const project of projects) {
      if (await claimProjectCatalogForOnboarding(qx, project.id)) {
        claimed.push(project)
        continue
      }
      const state = await resolveUnclaimedOutcome(qx, project)
      if (state === 'catalog-changed') {
        outcome.racedOut++
      } else {
        outcome.alreadyHandled++
      }
    }
  } catch (err) {
    await releaseClaims(qx, claimed)
    throw err
  }
  return claimed
}

function releaseClaims(qx: QueryExecutor, projects: IDbProjectCatalog[]): Promise<unknown> {
  return Promise.all(
    projects.map((project) => releaseProjectCatalogOnboardingClaim(qx, project.id)),
  )
}

function segmentIdOrThrow(result: IOnboardingResult): string | null {
  if (result.outcome === 'error') {
    throw new Error(result.error ?? 'Unknown onboarding error')
  }
  return result.segmentId
}

// Every claim this run made is released on any failure, so a retry sees the whole group again;
// the finalization is one transaction for the same reason.
async function onboardClaimedOrganization(
  qx: QueryExecutor,
  claimed: IDbProjectCatalog[],
): Promise<void> {
  log.info(
    { ids: claimed.map((project) => project.id), organization: organizationOf(claimed[0]) },
    'Starting the onboarding of an organization.',
  )
  try {
    const segmentId = segmentIdOrThrow(
      await runOrganizationProposedActionSerialized(claimed, qx, buildProposedActionDeps),
    )
    await qx.tx(async (tx) => {
      for (const project of claimed) {
        await markOnboarded(tx, project, segmentId)
      }
    })
    log.info(
      { ids: claimed.map((project) => project.id), segmentId },
      'Organization onboarding complete.',
    )
  } catch (err) {
    await releaseClaims(qx, claimed)
    throw err
  }
}

// Two runs claiming the rows of one new organization in turns would each create a project
// with their part; the claims are taken under one lock, in one transaction, before any call.
async function claimCreationGroup(
  qx: QueryExecutor,
  projects: IDbProjectCatalog[],
  outcome: IOrganizationOnboardingOutcome,
): Promise<IDbProjectCatalog[]> {
  const organizationKey = organizationGroupKey(projects[0])
  if (!organizationKey || !projects.every(createsNewProject)) {
    return claimOrganizationRows(qx, projects, outcome)
  }
  return qx.tx(async (tx) => {
    await lockProjectCatalogOnboardingTarget(tx, organizationKey)
    return claimOrganizationRows(tx, projects, outcome)
  })
}

export async function onboardAndUpdateOrganization(
  projects: IDbProjectCatalog[],
): Promise<IOrganizationOnboardingOutcome> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const outcome: IOrganizationOnboardingOutcome = { onboarded: [], alreadyHandled: 0, racedOut: 0 }
  const claimed = await claimCreationGroup(qx, projects, outcome)
  if (claimed.length === 0) {
    return outcome
  }
  await onboardClaimedOrganization(qx, claimed)
  outcome.onboarded = claimed
  return outcome
}

export async function onboardAndUpdateProject(
  project: IDbProjectCatalog,
): Promise<OnboardAndUpdateProjectOutcome> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const startTime = Date.now()

  const claim = await claimForOnboarding(qx, project)
  if (claim !== 'claimed') {
    return claim
  }

  log.info(
    { id: project.id, repoUrl: project.repoUrl, proposedAction: project.proposedAction },
    'Starting onboarding.',
  )

  const segmentId = await runProposedActionSerialized(project, qx, buildProposedActionDeps)
    .then(segmentIdOrThrow)
    .catch(async (err) => {
      await releaseProjectCatalogOnboardingClaim(qx, project.id)
      throw err
    })

  await markOnboarded(qx, project, segmentId)

  const elapsedSeconds = ((Date.now() - startTime) / 1000).toFixed(1)

  log.info(
    { id: project.id, repoUrl: project.repoUrl, segmentId, elapsedSeconds },
    'Onboarding complete.',
  )

  return 'onboarded'
}

async function replyInBotThread(
  project: Pick<IDbProjectCatalog, 'id' | 'repoUrl' | 'sourceUrl' | 'requestHints'>,
  content: string | ISlackMessageContent,
  status: BotStatus,
): Promise<void> {
  const result = await reportRequestOutcome(
    pgpQx(svc.postgres.writer.connection()),
    project,
    content,
  )

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

function reviewCardChannel(project: Pick<IDbProjectCatalog, 'sourceUrl'>): string | null {
  const origin = project.sourceUrl ? parseSlackPermalink(project.sourceUrl) : null
  return origin?.channel ?? getSlackBotConfig().reviewChannelId ?? null
}

async function closeReviewCard(
  project: IDbProjectCatalog,
  projectName: string,
  closedCards: Set<string>,
): Promise<void> {
  const channel = reviewCardChannel(project)
  if (!project.reviewMessageTs || !channel || closedCards.has(project.reviewMessageTs)) {
    return
  }

  const siblings = await findOrganizationSiblings(
    pgpQx(svc.postgres.writer.connection()),
    project,
    (row) => row.reviewMessageTs === project.reviewMessageTs,
  )
  const members = [project, ...siblings]
  const pending = members.filter((row) => row.action === 'onboard')
  if (pending.length > 0) {
    log.info(
      { id: project.id, ts: project.reviewMessageTs, pending: pending.length },
      'Organization card stays open while repositories are still being onboarded.',
    )
    return
  }

  const organization = organizationReviewOf(project, siblings.length)
  const onboarded = members.filter((row) => row.action === 'onboarded').length
  const card = toReviewRequestCard(project, NO_REVIEW_CARD_EXTRAS, organization)
  const closed = buildReviewClosedMessage(card, {
    projectName,
    reviewedBy: project.reviewedBy,
    reviewedAt: project.reviewedAt,
    repositories: { onboarded, total: members.length },
  })
  const result = await updateSlackMessage({ channel, ts: project.reviewMessageTs, ...closed })
  if (!result.ok) {
    log.warn(
      { id: project.id, repoUrl: project.repoUrl, channel, error: result.error },
      'Review card was not updated after the onboarding.',
    )
    return
  }
  closedCards.add(project.reviewMessageTs)
}

async function notifyOnboardedRequest(
  stale: IDbProjectCatalog,
  closedCards: Set<string>,
): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const project = (await findProjectCatalogById(qx, stale.id)) ?? stale
  const insightsProject = await findOnboardedInsightsProject(qx, project).catch((err) => {
    log.warn({ id: project.id, repoUrl: project.repoUrl, err }, 'Insights project lookup failed.')
    return null
  })

  if (isSlackBotProvenance(project.provenance)) {
    await replyInBotThread(project, buildBotOnboardedContent(project, insightsProject), 'added')
  }
  if (insightsProject) {
    await closeReviewCard(project, insightsProject.name, closedCards)
  }
}

export async function notifyOnboardedHumanRequests(projects: IDbProjectCatalog[]): Promise<void> {
  const closedCards = new Set<string>()
  for (const project of projects) {
    await notifyOnboardedRequest(project, closedCards)
  }

  for (const group of groupGithubDiscussionRequestsBySource(projects)) {
    const sent = await sendSlackNotificationAsync(
      SlackChannel.CDP_PROJECT_CATALOG_SKIP_ALERTS,
      SlackPersona.SUCCESS_ANNOUNCER,
      buildOnboardedDiscussionTitle(group),
      buildOnboardedDiscussionAlert(group),
    )

    if (!sent) {
      log.warn(
        { ids: group.map((project) => project.id), sourceUrl: group[0].sourceUrl },
        'Onboarded-discussion Slack alert was not sent.',
      )
    }
  }
}

export async function markProjectOnboardingFailed(
  projectId: string,
  reason: string,
): Promise<boolean> {
  const qx = pgpQx(svc.postgres.writer.connection())

  const updatedRows = await markProjectCatalogOnboardingFailed(qx, projectId, reason)

  if (updatedRows === 0) {
    log.info(
      { id: projectId },
      'Project was already onboarded or no longer pending, not marking as error.',
    )
    return false
  }

  log.error({ id: projectId, reason }, 'Onboarding permanently failed, marked as error.')
  return true
}

export async function notifySkippedBotRequest(project: IDbProjectCatalog): Promise<void> {
  if (!isSlackBotProvenance(project.provenance)) {
    return
  }

  const qx = pgpQx(svc.postgres.writer.connection())
  const current = await findProjectCatalogById(qx, project.id)
  if (current?.action !== 'skip') {
    log.info(
      { id: project.id, repoUrl: project.repoUrl, action: current?.action },
      'Row is no longer skipped, not replying in the Slack thread.',
    )
    return
  }

  const reason = current.skipReason ?? '(no reason recorded)'
  await replyInBotThread(project, buildBotSkippedReply(project, reason), skippedStatus(current))
  await closeOrganizationCardIfSettled(current)
}

// The last repository of an organization to fail or be skipped must still close the shared card.
async function closeOrganizationCardIfSettled(stale: IDbProjectCatalog): Promise<void> {
  if (!organizationOf(stale) || !stale.reviewMessageTs) {
    return
  }
  const qx = pgpQx(svc.postgres.writer.connection())
  const project = (await findProjectCatalogById(qx, stale.id)) ?? stale
  const projectName = project.matchedProjectName ?? project.projectName ?? 'the project'
  await closeReviewCard(project, projectName, new Set())
}

export async function notifyErroredHumanRequest(
  project: IDbProjectCatalog,
  reason: string,
): Promise<void> {
  if (isSlackBotProvenance(project.provenance)) {
    await replyInBotThread(project, buildBotErroredReply(project), 'problem')
    await closeOrganizationCardIfSettled(project)
    return
  }

  if (!isGithubDiscussionRequest(project)) {
    return
  }

  const sent = await sendSlackNotificationAsync(
    SlackChannel.CDP_PROJECT_CATALOG_SKIP_ALERTS,
    SlackPersona.ERROR_REPORTER,
    `Onboarding failed — ${project.repoName}`,
    buildErroredDiscussionAlert(project, reason),
  )

  if (!sent) {
    log.warn(
      { id: project.id, repoUrl: project.repoUrl },
      'Errored-discussion Slack alert was not sent.',
    )
  }
}

export async function startOnboardingPipelineRun(
  workflowId: string | null,
  temporalRunId: string | null,
): Promise<string> {
  const qx = pgpQx(svc.postgres.writer.connection())

  const run = await startPipelineRun(qx, { stage: 'onboarding', workflowId, temporalRunId })

  return run.id
}

export async function finishOnboardingPipelineRun(
  id: string,
  data: IPipelineRunFinish,
): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())

  await finishPipelineRun(qx, id, data)
}
