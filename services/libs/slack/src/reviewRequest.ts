import type { Button, KnownBlock } from '@slack/web-api'

import { githubRepoPath } from '@crowd/common'
import { getServiceLogger } from '@crowd/logging'

import { getSlackBotConfig } from './botConfig'
import { getSlackPermalink, postSlackMessage } from './chatClient'
import { escapeSlackText, formatSlackTime } from './mrkdwn'
import { describeDecisionReason } from './reviewRejectModal'
import { parseSlackPermalink, postSlackContentInThread } from './threadReply'

const log = getServiceLogger()

export const APPROVE_REVIEW_ACTION_ID = 'approve_review'
export const REJECT_REVIEW_ACTION_ID = 'reject_review'
export const MARK_DONE_REVIEW_ACTION_ID = 'mark_done_review'
export const OPEN_IN_CDP_ACTION_ID = 'open_in_cdp'

export type ReviewDecision = 'approve' | 'reject' | 'done'

export type ReviewCardAction = 'create_project' | 'add_to_connection' | 'connect_github' | 'manual'

export interface IReviewRepoContext {
  description: string | null
  stars: number
  pushedAt: string | null
  isArchived: boolean
  isFork: boolean
  parentFullName: string | null
}

export interface IReviewOrganization {
  owner: string
  repositories: number
}

export function organizationLabel(owner: string): string {
  return `github.com/${owner}`
}

export function repositoriesWord(count: number): string {
  return count === 1 ? 'repository' : 'repositories'
}

export interface IReviewRequestCard {
  id: string
  repoName: string
  repoUrl: string
  sourceUrl: string | null
  organization?: IReviewOrganization | null
  repoContext: IReviewRepoContext | null
  requestedName: string | null
  matchedName: string | null
  insightsState: string
  whatsNeeded: string
  mismatch: boolean
  action: ReviewCardAction
  evaluationNote: string | null
  autoApplyHold: boolean
  cdpUrl: string | null
  details: string | null
}

export interface IReviewDecisionMessage {
  text: string
  blocks: KnownBlock[]
}

const SLACK_PERMALINK_PATTERN = /^https:\/\/[^/]+\.slack\.com\/archives\//
const MAX_BUTTON_TEXT_LENGTH = 75
const MISMATCH_NOTICE =
  ':warning: Requested name and matched project differ. Check before going ahead.'
const FALLBACK_PROJECT_LABEL = 'the project'
const MAX_DESCRIPTION_LENGTH = 120
const MAX_EVALUATION_NOTE_LENGTH = 300
const AUTO_APPLY_HOLD_NOTICE =
  ':gear: Auto-apply is off, so this needs an approval. With it on, this would run by itself.'

function formatRequestedIn(sourceUrl: string | null): string {
  if (!sourceUrl) {
    return '_source not recorded_'
  }

  return SLACK_PERMALINK_PATTERN.test(sourceUrl)
    ? `<${sourceUrl}|Slack request>`
    : `<${sourceUrl}|${escapeSlackText(sourceUrl)}>`
}

function formatRepoUrl(repoUrl: string): string {
  return `\`${escapeSlackText(repoUrl)}\``
}

function countRepositories(repositories: number): string {
  return `${repositories} ${repositoriesWord(repositories)}`
}

export function organizationSubjectText({ owner, repositories }: IReviewOrganization): string {
  return `${organizationLabel(owner)} (${countRepositories(repositories)})`
}

export function organizationHeading({ owner, repositories }: IReviewOrganization): string {
  return `*${escapeSlackText(organizationLabel(owner))}* · ${countRepositories(repositories)}`
}

function reposPhrase(card: IReviewRequestCard): string {
  const organization = card.organization
  if (!organization) {
    return formatRepoUrl(card.repoUrl)
  }
  return `the ${countRepositories(organization.repositories)} of ${formatRepoUrl(`https://github.com/${organization.owner}`)}`
}

function reposWord(card: IReviewRequestCard): string {
  if (!card.organization) {
    return 'repo'
  }
  const { repositories } = card.organization
  return `${repositories} ${repositories === 1 ? 'repo' : 'repos'}`
}

function contextBlock(text: string): KnownBlock {
  return { type: 'context', elements: [{ type: 'mrkdwn', text }] }
}

function detailsBlock(details: string | null): KnownBlock[] {
  return details ? [contextBlock(escapeSlackText(details))] : []
}

function isPostedInRequestThread(card: IReviewRequestCard): boolean {
  return !!card.sourceUrl && SLACK_PERMALINK_PATTERN.test(card.sourceUrl)
}

function field(label: string, value: string): { type: 'mrkdwn'; text: string } {
  return { type: 'mrkdwn', text: `*${label}*\n${value}` }
}

function shortDescription(description: string): string {
  return description.length <= MAX_DESCRIPTION_LENGTH
    ? description
    : `${description.slice(0, MAX_DESCRIPTION_LENGTH).trimEnd()}…`
}

function forkLabel(context: IReviewRepoContext): string {
  return context.parentFullName ? `*fork of ${escapeSlackText(context.parentFullName)}*` : '*fork*'
}

export function formatRepoContext(context: IReviewRepoContext): string {
  const parts = [
    context.description ? escapeSlackText(shortDescription(context.description)) : null,
    `★ ${context.stars.toLocaleString('en-US')}`,
    context.pushedAt ? `last push ${context.pushedAt.slice(0, 10)}` : null,
    context.isFork ? forkLabel(context) : null,
    context.isArchived ? '*archived*' : null,
  ]
  return parts.filter((part): part is string => part !== null).join(' · ')
}

function repoContextBlock(context: IReviewRepoContext | null): KnownBlock[] {
  return context ? [contextBlock(formatRepoContext(context))] : []
}

function shortNote(note: string): string {
  return note.length <= MAX_EVALUATION_NOTE_LENGTH
    ? note
    : `${note.slice(0, MAX_EVALUATION_NOTE_LENGTH).trimEnd()}…`
}

function evaluationNoteBlock(note: string | null): KnownBlock[] {
  const trimmed = note?.trim()
  return trimmed
    ? [contextBlock(`:robot_face: Automatic check: ${escapeSlackText(shortNote(trimmed))}`)]
    : []
}

function headingLines(card: IReviewRequestCard): string[] {
  return card.organization
    ? [
        organizationHeading(card.organization),
        escapeSlackText(`https://github.com/${card.organization.owner}`),
      ]
    : [`*${escapeSlackText(card.repoName)}*`, escapeSlackText(card.repoUrl)]
}

function summaryBlocks(card: IReviewRequestCard): KnownBlock[] {
  const lines = headingLines(card)
  if (!isPostedInRequestThread(card)) {
    lines.push(`Requested in: ${formatRequestedIn(card.sourceUrl)}`)
  }
  return [
    { type: 'section', text: { type: 'mrkdwn', text: lines.join('\n') } },
    ...repoContextBlock(card.organization ? null : card.repoContext),
    {
      type: 'section',
      fields: [
        field(
          'Requested as',
          card.requestedName ? escapeSlackText(card.requestedName) : '_not provided_',
        ),
        field('Matched to', card.matchedName ? escapeSlackText(card.matchedName) : '_no match_'),
        field('In LFX Insights', escapeSlackText(card.insightsState)),
        field("What's needed", escapeSlackText(card.whatsNeeded)),
      ],
    },
    ...(card.mismatch ? [contextBlock(MISMATCH_NOTICE)] : []),
    ...evaluationNoteBlock(card.evaluationNote),
    ...detailsBlock(card.details),
  ]
}

function buttonText(text: string): string {
  return text.length > MAX_BUTTON_TEXT_LENGTH
    ? `${text.slice(0, MAX_BUTTON_TEXT_LENGTH - 1)}…`
    : text
}

function matchedLabel(card: IReviewRequestCard): string {
  return card.matchedName ?? FALLBACK_PROJECT_LABEL
}

function confirmDialog(title: string, text: string, confirm: string): Button['confirm'] {
  return {
    title: { type: 'plain_text', text: title },
    text: { type: 'mrkdwn', text },
    confirm: { type: 'plain_text', text: confirm },
    deny: { type: 'plain_text', text: 'Cancel' },
  }
}

function approveButton(card: IReviewRequestCard, text: string, confirmText: string): Button {
  return {
    type: 'button',
    action_id: APPROVE_REVIEW_ACTION_ID,
    value: card.id,
    style: 'primary',
    text: { type: 'plain_text', text: buttonText(text) },
    confirm: confirmDialog('Approve onboarding?', confirmText, 'Approve'),
  }
}

function manualButtons(card: IReviewRequestCard): Button[] {
  const openInCdp: Button[] = card.cdpUrl
    ? [
        {
          type: 'button',
          action_id: OPEN_IN_CDP_ACTION_ID,
          url: card.cdpUrl,
          text: { type: 'plain_text', text: 'Open in CDP ↗' },
        },
      ]
    : []
  return [
    ...openInCdp,
    {
      type: 'button',
      action_id: MARK_DONE_REVIEW_ACTION_ID,
      value: card.id,
      style: 'primary',
      text: { type: 'plain_text', text: 'Mark as done' },
      confirm: confirmDialog(
        'Mark as done?',
        card.organization
          ? 'Only after the repositories were added by hand in CDP. The requester will be told it is done.'
          : 'Only after the repo was added by hand in CDP. The requester will be told it is done.',
        'Mark as done',
      ),
    },
  ]
}

function decisionButtons(card: IReviewRequestCard): Button[] {
  const matched = escapeSlackText(matchedLabel(card))
  switch (card.action) {
    case 'create_project':
      return [
        approveButton(
          card,
          `Create project and add ${reposWord(card)}`,
          `A new LFX Insights project will be created for ${reposPhrase(card)}.`,
        ),
      ]
    case 'add_to_connection':
      return [
        approveButton(
          card,
          card.organization
            ? `Add ${reposWord(card)} to ${matchedLabel(card)}`
            : `Add to ${matchedLabel(card)}`,
          `${reposPhrase(card)} will be added to ${matched}'s GitHub connection.`,
        ),
      ]
    case 'connect_github':
      return [
        approveButton(
          card,
          `Connect GitHub and add ${reposWord(card)}`,
          `GitHub will be connected to ${matched} with ${reposPhrase(card)}.`,
        ),
      ]
    case 'manual':
      return manualButtons(card)
  }
}

function rejectButton(card: IReviewRequestCard): Button {
  return {
    type: 'button',
    action_id: REJECT_REVIEW_ACTION_ID,
    value: card.id,
    style: 'danger',
    text: { type: 'plain_text', text: 'Reject' },
  }
}

export function buildReviewRequestBlocks(card: IReviewRequestCard): KnownBlock[] {
  return [
    ...summaryBlocks(card),
    ...(card.autoApplyHold ? [contextBlock(AUTO_APPLY_HOLD_NOTICE)] : []),
    { type: 'actions', elements: [...decisionButtons(card), rejectButton(card)] },
  ]
}

function cardSubject(card: IReviewRequestCard): string {
  if (card.organization) {
    return escapeSlackText(organizationSubjectText(card.organization))
  }
  return escapeSlackText(card.repoName)
}

export function buildReviewRequestText(card: IReviewRequestCard): string {
  if (card.organization) {
    return `Review needed — ${cardSubject(card)}`
  }
  return `Review needed — ${escapeSlackText(githubRepoPath(card.repoUrl) ?? card.repoName)}`
}

export interface IReviewDecisionOptions {
  onboardingStarted: boolean
  reviewedAt?: string | null
  decisionReason?: string | null
}

function rejectionOutcome(reviewedBy: string, options: IReviewDecisionOptions): string {
  const at = options.reviewedAt ? ` at ${formatSlackTime(options.reviewedAt)}` : ''
  const reason = describeDecisionReason(options.decisionReason ?? null)
  return reason
    ? `:no_entry_sign: Rejected by <@${reviewedBy}>${at} · ${reason}`
    : `:no_entry_sign: Rejected by <@${reviewedBy}>${at}, the repository was skipped.`
}

function decisionOutcome(
  decision: ReviewDecision,
  reviewedBy: string,
  options: IReviewDecisionOptions,
): string {
  const onboardingStarted = options.onboardingStarted
  switch (decision) {
    case 'reject':
      return rejectionOutcome(reviewedBy, options)
    case 'done':
      return `:white_check_mark: Marked as done by <@${reviewedBy}>, added by hand in CDP.`
    case 'approve':
      return onboardingStarted
        ? `:white_check_mark: Approved by <@${reviewedBy}>, onboarding started.`
        : `:white_check_mark: Approved by <@${reviewedBy}>, queued for the next onboarding run.`
  }
}

const DECISION_TITLES: Record<ReviewDecision, string> = {
  approve: 'Approved',
  reject: 'Rejected',
  done: 'Done',
}

export function buildReviewDecisionMessage(
  card: IReviewRequestCard,
  decision: ReviewDecision,
  reviewedBy: string,
  options: IReviewDecisionOptions,
): IReviewDecisionMessage {
  const outcome = decisionOutcome(decision, reviewedBy, options)

  return {
    text: `${DECISION_TITLES[decision]} — ${cardSubject(card)}`,
    blocks: [...summaryBlocks(card), contextBlock(outcome)],
  }
}

export interface IReviewClosure {
  projectName: string
  reviewedBy: string | null
  reviewedAt: string | null
  repositories?: { onboarded: number; total: number }
}

function closureOutcome(closure: IReviewClosure, who: string): { title: string; outcome: string } {
  const project = `*${escapeSlackText(closure.projectName)}*`
  const repositories = closure.repositories
  if (repositories && repositories.onboarded === 0) {
    return {
      title: 'Failed',
      outcome:
        repositories.total === 1
          ? `:warning: Could not be added to ${project}${who}`
          : `:warning: None of the ${repositories.total} repositories could be added to ${project}${who}`,
    }
  }
  if (repositories && repositories.onboarded < repositories.total) {
    const failed = repositories.total - repositories.onboarded
    return {
      title: 'Added',
      outcome: `:white_check_mark: Added ${repositories.onboarded} of ${repositories.total} repositories to ${project}${who} · ${failed} not added`,
    }
  }
  return { title: 'Added', outcome: `:white_check_mark: Added to ${project}${who}` }
}

export function buildReviewClosedMessage(
  card: IReviewRequestCard,
  closure: IReviewClosure,
): IReviewDecisionMessage {
  const by = closure.reviewedBy ? ` by <@${closure.reviewedBy}>` : ''
  const at = closure.reviewedAt ? ` at ${formatSlackTime(closure.reviewedAt)}` : ''
  const { title, outcome } = closureOutcome(closure, `${by}${at}`)

  return {
    text: `${title} — ${cardSubject(card)}`,
    blocks: [...summaryBlocks(card), contextBlock(outcome)],
  }
}

export interface IReviewRequestPostResult {
  ok: boolean
  ts?: string
  error?: string
}

async function postReviewNotice(
  reviewChannelId: string,
  card: IReviewRequestCard,
  cardChannel: string,
  cardTs: string,
): Promise<void> {
  const permalink = await getSlackPermalink(cardChannel, cardTs)
  if (!permalink) {
    log.warn({ cardId: card.id }, 'Review notice skipped: no permalink for the thread card.')
    return
  }
  const result = await postSlackMessage({
    channel: reviewChannelId,
    text: `${buildReviewRequestText(card)} · <${permalink}|Open the request>`,
  })
  if (!result.ok) {
    log.warn(
      { cardId: card.id, error: result.error },
      'Review notice was not posted to the channel.',
    )
  }
}

export async function postReviewRequest(
  card: IReviewRequestCard,
  origin: string | null,
): Promise<IReviewRequestPostResult> {
  const { reviewChannelId } = getSlackBotConfig()
  const content = { text: buildReviewRequestText(card), blocks: buildReviewRequestBlocks(card) }

  if (origin && parseSlackPermalink(origin)) {
    const posted = await postSlackContentInThread(origin, content)
    if (posted.ok && posted.ts && posted.channel && reviewChannelId) {
      await postReviewNotice(reviewChannelId, card, posted.channel, posted.ts)
    }
    return posted
  }

  if (!reviewChannelId) {
    return { ok: false, error: 'Slack review channel not configured' }
  }

  return postSlackMessage({ channel: reviewChannelId, ...content })
}
