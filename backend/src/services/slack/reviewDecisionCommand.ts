import { z } from 'zod'

import { findProjectCatalogById, resolveProjectCatalogReview } from '@crowd/data-access-layer'
import {
  IDbProjectCatalog,
  IDbProjectCatalogReviewResolution,
  isSlackBotProvenance,
} from '@crowd/data-access-layer/src/project-catalog/types'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import {
  NO_REVIEW_CARD_EXTRAS,
  applyRequestStatus,
  refreshRequestStatusMessage,
  reportRequestOutcome,
  buildBotDoneReply,
  buildBotRejectedReply,
  resolveReviewCdpUrl,
  findOrganizationSiblings,
  needsPerson,
  organizationReviewOf,
  toReviewRequestCard,
} from '@crowd/project-onboarding'
import {
  IReviewOrganization,
  NOT_ALLOWED_NOTICE,
  ReviewDecision,
  buildNotAllowedModal,
  buildRejectReviewModal,
  buildReviewDecisionMessage,
  buildReviewRequestBlocks,
  buildReviewRequestText,
  organizationSubjectText,
  describeDecisionReason,
  formatSlackTime,
  getSlackBotConfig,
  openSlackView,
} from '@crowd/slack'
import { WorkflowIdReusePolicy } from '@crowd/temporal'

import { API_CONFIG } from '../../conf'
import { IServiceOptions } from '../IServiceOptions'
import type { OnboardingRequestTemporal } from './onboardingRequestBot'
import { ResponseUrlMessage, postToResponseUrl, textMessage } from './onboardProjectCommand'
import { getBgQx } from './slackBackground'

const catalogIdSchema = z.string().uuid()

export interface IReviewDecisionReason {
  decisionReason: string
  decisionNote: string | null
}

export interface IReviewDecisionCommand {
  catalogId: string
  decision: ReviewDecision
  reason?: IReviewDecisionReason
  responseUrl: string
  actorId: string
  triggerId: string | null
  temporal: OnboardingRequestTemporal | null
  log: IServiceOptions['log']
}

export interface IReviewRejectPrompt {
  catalogId: string
  responseUrl: string
  actorId: string
  triggerId: string | null
  log: IServiceOptions['log']
}

function replaceOriginal(
  responseUrl: string,
  message: ResponseUrlMessage,
  log: IServiceOptions['log'],
) {
  const replaced = { ...message, replace_original: true }
  return postToResponseUrl(responseUrl, replaced, log)
}

function isAllowedReviewer(actorId: string, log: IServiceOptions['log']): boolean {
  const { reviewerUserIds } = getSlackBotConfig()
  if (reviewerUserIds.length === 0) {
    log.warn('CROWD_SLACK_REVIEWER_USER_IDS is not set, every review decision is refused.')
    return false
  }
  return reviewerUserIds.includes(actorId)
}

function postEphemeral(responseUrl: string, text: string, log: IServiceOptions['log']) {
  const ephemeral = { ...textMessage(text), response_type: 'ephemeral', replace_original: false }
  return postToResponseUrl(responseUrl, ephemeral, log)
}

async function notifyNotAllowed(
  responseUrl: string,
  triggerId: string | null,
  log: IServiceOptions['log'],
): Promise<void> {
  const opened = triggerId ? await openSlackView(triggerId, buildNotAllowedModal()) : { ok: false }
  if (!opened.ok) {
    await postEphemeral(responseUrl, NOT_ALLOWED_NOTICE, log)
  }
}

async function rejectInvalidCatalogId(
  rawCatalogId: string,
  responseUrl: string,
  log: IServiceOptions['log'],
): Promise<string | null> {
  const parsedId = catalogIdSchema.safeParse(rawCatalogId)
  if (parsedId.success) {
    return parsedId.data
  }
  log.warn({ rawCatalogId }, 'Review decision click carried an invalid catalog id.')
  await replaceOriginal(
    responseUrl,
    textMessage(':no_entry: This request is no longer valid.'),
    log,
  )
  return null
}

export async function runReviewRejectPrompt({
  catalogId: rawCatalogId,
  responseUrl,
  actorId,
  triggerId,
  log,
}: IReviewRejectPrompt): Promise<void> {
  const catalogId = await rejectInvalidCatalogId(rawCatalogId, responseUrl, log)
  if (!catalogId) {
    return
  }

  if (!isAllowedReviewer(actorId, log)) {
    log.info({ catalogId, actorId }, 'Reject click from a non-reviewer ignored.')
    await notifyNotAllowed(responseUrl, triggerId, log)
    return
  }

  const qx = await getBgQx()
  const row = await findProjectCatalogById(qx, catalogId)
  if (!row || row.action !== 'review') {
    await explainDecidedRow(row, responseUrl, log)
    return
  }

  if (!triggerId) {
    log.warn({ catalogId }, 'Reject click without a trigger id, cannot open the reason prompt.')
    await postEphemeral(responseUrl, 'The reject form could not be opened. Please try again.', log)
    return
  }

  const opened = await openSlackView(
    triggerId,
    buildRejectReviewModal({
      catalogId,
      repoName: rejectSubject(row, await pendingOrganizationReview(qx, row)),
      responseUrl,
    }),
  )
  if (!opened.ok) {
    log.warn({ catalogId, error: opened.error }, 'The reject prompt could not be opened.')
    await postEphemeral(responseUrl, 'The reject form could not be opened. Please try again.', log)
  }
}

export function postReviewDecisionFailure(responseUrl: string, log: IServiceOptions['log']) {
  return postToResponseUrl(
    responseUrl,
    textMessage(':no_entry: Recording the review decision failed. Please try again.'),
    log,
  )
}

function reviewCycleWorkflowId(catalogId: string, reviewedAt: string | null): string {
  const cycle = reviewedAt ? Date.parse(reviewedAt) : Date.now()
  return `onboard-review-${catalogId}-${cycle}`
}

function onSameReviewCard(lead: IDbProjectCatalog): (row: IDbProjectCatalog) => boolean {
  const cardTs = lead.reviewMessageTs
  return (row) => !!cardTs && row.action === 'review' && row.reviewMessageTs === cardTs
}

async function decideOrganizationSiblings(
  qx: QueryExecutor,
  lead: IDbProjectCatalog,
  resolution: IDbProjectCatalogReviewResolution,
  log: IServiceOptions['log'],
): Promise<IDbProjectCatalog[]> {
  const siblings = await findOrganizationSiblings(qx, lead, onSameReviewCard(lead))
  const decided: IDbProjectCatalog[] = []
  for (const sibling of siblings) {
    const resolved = await resolveProjectCatalogReview(qx, sibling.id, resolution)
    if (resolved) {
      decided.push(resolved)
    } else {
      log.warn({ catalogId: sibling.id, leadId: lead.id }, 'Organization sibling was not decided.')
    }
  }
  return decided
}

async function startApprovedOnboarding(
  temporal: OnboardingRequestTemporal | null,
  catalogIds: string[],
  reviewedAt: string | null,
  log: IServiceOptions['log'],
): Promise<boolean> {
  const [catalogId] = catalogIds
  if (!temporal) {
    log.warn({ catalogIds }, 'Temporal not configured, the approved rows wait for the schedule.')
    return false
  }

  try {
    await temporal.workflow.start('onboardProjects', {
      taskQueue: 'automatic-onboarding',
      workflowId: reviewCycleWorkflowId(catalogId, reviewedAt),
      workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
      args: [{ catalogIds }],
    })
    return true
  } catch (err) {
    if (err instanceof Error && err.name === 'WorkflowExecutionAlreadyStartedError') {
      return true
    }
    log.error(
      { catalogIds, err },
      'Starting the approved onboarding failed, the rows wait for the schedule.',
    )
    return false
  }
}

export async function runReviewDecisionCommand({
  catalogId: rawCatalogId,
  decision,
  reason,
  responseUrl,
  actorId,
  triggerId,
  temporal,
  log,
}: IReviewDecisionCommand): Promise<void> {
  const catalogId = await rejectInvalidCatalogId(rawCatalogId, responseUrl, log)
  if (!catalogId) {
    return
  }

  if (!isAllowedReviewer(actorId, log)) {
    log.info({ catalogId, decision, actorId }, 'Review decision click from a non-reviewer ignored.')
    await notifyNotAllowed(responseUrl, triggerId, log)
    return
  }

  const qx = await getBgQx()
  const resolution: IDbProjectCatalogReviewResolution = {
    decision,
    reviewedBy: actorId,
    decisionReason: reason?.decisionReason ?? null,
    decisionNote: reason?.decisionNote ?? null,
  }
  const { resolved, siblings } = await qx.tx(async (tx) => {
    const lead = await resolveProjectCatalogReview(tx, catalogId, resolution)
    if (!lead) {
      return { resolved: null, siblings: [] as IDbProjectCatalog[] }
    }
    return { resolved: lead, siblings: await decideOrganizationSiblings(tx, lead, resolution, log) }
  })
  if (!resolved) {
    await handleRefusedDecision(qx, catalogId, decision, responseUrl, actorId, log)
    return
  }
  const organization = organizationReviewOf(resolved, siblings.length)
  const catalogIds = [resolved.id, ...siblings.map((row) => row.id)]

  log.info(
    { catalogId, repoUrl: resolved.repoUrl, decision, actorId, siblings: siblings.length },
    'Review decision recorded.',
  )

  const replaceCard = (onboardingStarted: boolean) =>
    replaceOriginal(
      responseUrl,
      buildReviewDecisionMessage(
        toReviewRequestCard(resolved, NO_REVIEW_CARD_EXTRAS, organization),
        decision,
        actorId,
        {
          onboardingStarted,
          reviewedAt: resolved.reviewedAt,
          decisionReason: resolved.decisionReason,
        },
      ),
      log,
    )

  // The card must be replaced before the onboarding starts, or its closure could be overwritten.
  const expectsStart = decision === 'approve' && temporal !== null
  await replaceCard(expectsStart)
  if (decision === 'approve') {
    // Status and reaction go before the start: a fast onboarding must not be undone by a late ⏳.
    if (isSlackBotProvenance(resolved.provenance)) {
      await refreshRequestStatusMessage(qx, resolved)
      await applyRequestStatus(qx, resolved, 'checking')
    }
    const started = await startApprovedOnboarding(temporal, catalogIds, resolved.reviewedAt, log)
    if (started !== expectsStart) {
      await replaceCard(started)
    }
  }

  if (decision === 'reject') {
    await replyInBotThread(
      qx,
      resolved,
      buildBotRejectedReply(resolved, organization),
      'notAdded',
      log,
    )
  } else if (decision === 'done') {
    await replyInBotThread(qx, resolved, buildBotDoneReply(resolved, organization), 'added', log)
  }
}

const REFUSED_DECISION_NOTICES: Record<ReviewDecision, string> = {
  approve:
    'This request needs a person: add the repo by hand in CDP, then use Mark as done. The buttons were refreshed.',
  done: "This repo is added automatically, so it can't be marked as done. The buttons were refreshed.",
  reject: 'This request could not be rejected. The buttons were refreshed, please try again.',
}

function decidedVerb(row: IDbProjectCatalog): string {
  switch (row.action) {
    case 'skip':
      return 'rejected'
    case 'onboarded':
      return needsPerson(row) ? 'marked as done' : 'approved'
    default:
      return 'approved'
  }
}

export function describeDecidedRow(row: IDbProjectCatalog): string {
  const at = row.reviewedAt ? ` at ${formatSlackTime(row.reviewedAt)}` : ''
  const reason = describeDecisionReason(row.decisionReason)
  return `<@${row.reviewedBy}> already ${decidedVerb(row)} this${at}${reason ? ` (${reason})` : ''}.`
}

async function explainDecidedRow(
  row: IDbProjectCatalog | null,
  responseUrl: string,
  log: IServiceOptions['log'],
): Promise<void> {
  const notice =
    row?.reviewedBy && row.action !== 'review'
      ? describeDecidedRow(row)
      : 'This request was already reviewed or is no longer pending review.'
  await postEphemeral(responseUrl, notice, log)
}

async function handleRefusedDecision(
  qx: QueryExecutor,
  catalogId: string,
  decision: ReviewDecision,
  responseUrl: string,
  actorId: string,
  log: IServiceOptions['log'],
): Promise<void> {
  const row = await findProjectCatalogById(qx, catalogId)
  if (!row || row.action !== 'review') {
    log.info({ catalogId, decision, actorId }, 'Review decision for a row no longer in review.')
    await explainDecidedRow(row, responseUrl, log)
    return
  }

  log.info({ catalogId, decision, actorId }, 'Review decision refused, the card was refreshed.')
  const card = toReviewRequestCard(
    row,
    { cdpUrl: await resolveReviewCdpUrl(qx, row, API_CONFIG.frontendUrl), repoContext: null },
    await pendingOrganizationReview(qx, row),
  )
  await replaceOriginal(
    responseUrl,
    { text: buildReviewRequestText(card), blocks: buildReviewRequestBlocks(card) },
    log,
  )
  await postEphemeral(responseUrl, REFUSED_DECISION_NOTICES[decision], log)
}

function rejectSubject(row: IDbProjectCatalog, organization: IReviewOrganization | null): string {
  return organization ? organizationSubjectText(organization) : row.repoName
}

async function pendingOrganizationReview(qx: QueryExecutor, row: IDbProjectCatalog) {
  const siblings = await findOrganizationSiblings(qx, row, onSameReviewCard(row))
  return organizationReviewOf(row, siblings.length)
}

async function replyInBotThread(
  qx: QueryExecutor,
  row: IDbProjectCatalog,
  text: string,
  status: 'notAdded' | 'added',
  log: IServiceOptions['log'],
): Promise<void> {
  if (!isSlackBotProvenance(row.provenance)) {
    return
  }

  const result = await reportRequestOutcome(qx, row, text)
  if (!result.ok) {
    log.warn(
      { catalogId: row.id, sourceUrl: row.sourceUrl, error: result.error },
      'Review outcome reply in the Slack bot thread was not sent.',
    )
  }
  await applyRequestStatus(qx, row, status)
}
