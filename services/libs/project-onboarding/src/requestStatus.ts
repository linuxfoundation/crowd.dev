import {
  ISlackOnboardingRequestHeader,
  claimSlackOnboardingRequestDone,
  findProjectCatalogBySourceUrl,
  findSlackOnboardingRequest,
  lockSlackOnboardingRequest,
} from '@crowd/data-access-layer'
import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import {
  ISlackMessageContent,
  MAX_BLOCKS_PER_MESSAGE,
  escapeSlackText,
  linkButtonBlocks,
  mrkdwnSections,
  organizationHeading,
  postSlackContentInThread,
  updateSlackMessage,
} from '@crowd/slack'

import { BotStatus, STATUS, VIEW_INSIGHTS_PROJECT_ACTION_ID, repoLabel } from './botCopy'
import { notAddedReason } from './botIngestReply'
import { formatReason } from './botThreadReply'
import { organizationOf } from './organizationGroup'

const log = getServiceLogger()

export type RequestStatusRow = Pick<
  IDbProjectCatalog,
  | 'repoUrl'
  | 'action'
  | 'classificationKind'
  | 'reviewReason'
  | 'skipReason'
  | 'evaluationReason'
  | 'requestQuestion'
  | 'matchedProjectName'
  | 'reviewedBy'
  | 'decisionReason'
  | 'decisionNote'
  | 'requestHints'
>

type RequestRef = Pick<IDbProjectCatalog, 'id' | 'repoUrl' | 'sourceUrl'>

const CLOSING_LINE = "I'll update this message as each repo is checked."
const MAX_LINE_REASON_LENGTH = 160
const BUTTONS_PER_BLOCK = 5

export function rowStatus(row: Pick<RequestStatusRow, 'action' | 'classificationKind'>): BotStatus {
  switch (row.action) {
    case 'review':
      return 'withTeam'
    case 'ask':
      return 'needsInput'
    case 'onboarded':
      return 'added'
    case 'skip':
      return row.classificationKind === 'already_tracked' ? 'alreadyThere' : 'notAdded'
    case 'unsure':
    case 'error':
      return 'problem'
    default:
      return 'checking'
  }
}

function shortReason(reason: string): string {
  const formatted = formatReason(reason)
  return formatted.length > MAX_LINE_REASON_LENGTH
    ? `${formatted.slice(0, MAX_LINE_REASON_LENGTH).trimEnd()}…`
    : formatted
}

function quotedNote(note: string | null): string {
  const trimmed = note?.trim()
  return trimmed ? `\n> ${escapeSlackText(trimmed).split('\n').join('\n> ')}` : ''
}

function statusDetail(status: BotStatus, row: RequestStatusRow, plural = false): string {
  switch (status) {
    case 'withTeam':
      return `with the CDP team${row.reviewReason ? `: ${shortReason(row.reviewReason)}` : ''}`
    case 'needsInput':
      return `${plural ? 'need' : 'needs'} your input${row.requestQuestion ? `: ${escapeSlackText(row.requestQuestion.text)}` : ''}`
    case 'added':
      return `added${row.matchedProjectName ? ` to *${escapeSlackText(row.matchedProjectName)}*` : ''}`
    case 'alreadyThere':
      return 'already in LFX Insights'
    case 'notAdded':
      return `not added: ${notAddedReason({ ...row, skipReason: row.skipReason ?? row.evaluationReason })}${quotedNote(row.decisionNote)}`
    case 'problem':
      return 'something went wrong on our side'
    default:
      return 'checking…'
  }
}

export function requestStatusLine(row: RequestStatusRow): string {
  const status = rowStatus(row)
  return `${STATUS[status]} ${repoLabel(row.repoUrl)} · ${statusDetail(status, row)}`
}

const SUMMARY_LABELS: Record<BotStatus, string> = {
  added: 'added',
  alreadyThere: 'already there',
  withTeam: 'with the CDP team',
  needsInput: 'waiting for you',
  notAdded: 'not added',
  problem: 'failed',
  checking: 'checking',
}

const SUMMARY_ORDER: BotStatus[] = [
  'added',
  'alreadyThere',
  'withTeam',
  'needsInput',
  'notAdded',
  'problem',
  'checking',
]

export function requestSummaryLine(rows: RequestStatusRow[], alreadyTracked = 0): string {
  const counts = new Map<BotStatus, number>()
  for (const row of rows) {
    const status = rowStatus(row)
    counts.set(status, (counts.get(status) ?? 0) + 1)
  }
  if (alreadyTracked > 0) {
    counts.set('alreadyThere', (counts.get('alreadyThere') ?? 0) + alreadyTracked)
  }
  return SUMMARY_ORDER.filter((status) => counts.has(status))
    .map((status) => `${counts.get(status)} ${SUMMARY_LABELS[status]}`)
    .join(', ')
}

export function isRequestSettled(rows: RequestStatusRow[]): boolean {
  return rows.every((row) => !['checking', 'withTeam', 'needsInput'].includes(rowStatus(row)))
}

function mixedGroupStatus(rows: RequestStatusRow[]): BotStatus {
  if (!isRequestSettled(rows)) {
    return 'checking'
  }
  const present = new Set(rows.map(rowStatus))
  return SUMMARY_ORDER.find((status) => present.has(status)) ?? 'checking'
}

function sharedGroupDetail(rows: RequestStatusRow[]): string | null {
  const statuses = new Set(rows.map(rowStatus))
  if (statuses.size !== 1) {
    return null
  }
  const [status] = statuses
  const details = new Set(rows.map((row) => statusDetail(status, row, rows.length > 1)))
  return details.size === 1 ? [...details][0] : null
}

export function organizationStatusLine(owner: string, rows: RequestStatusRow[]): string {
  const label = organizationHeading({ owner, repositories: rows.length })
  const detail = sharedGroupDetail(rows)
  return detail
    ? `${STATUS[rowStatus(rows[0])]} ${label} · ${detail}`
    : `${STATUS[mixedGroupStatus(rows)]} ${label}: ${requestSummaryLine(rows)}`
}

interface IStatusEntry {
  owner: string | null
  rows: RequestStatusRow[]
}

function groupByOrganization(rows: RequestStatusRow[]): IStatusEntry[] {
  const entries: IStatusEntry[] = []
  const byOwner = new Map<string, IStatusEntry>()
  for (const row of rows) {
    const owner = organizationOf(row)
    const entry = owner ? byOwner.get(owner) : undefined
    if (entry) {
      entry.rows.push(row)
      continue
    }
    const created = { owner, rows: [row] }
    if (owner) {
      byOwner.set(owner, created)
    }
    entries.push(created)
  }
  return entries
}

function statusLines(rows: RequestStatusRow[]): string[] {
  return groupByOrganization(rows).map(({ owner, rows: group }) =>
    owner && group.length > 1 ? organizationStatusLine(owner, group) : requestStatusLine(group[0]),
  )
}

export interface IRequestStatusInput {
  header: ISlackOnboardingRequestHeader
  rows: RequestStatusRow[]
  alreadyTracked?: number
  closing?: string | null
}

export function buildRequestStatusContent({
  header,
  rows,
  alreadyTracked = 0,
  closing = null,
}: IRequestStatusInput): ISlackMessageContent {
  const lines = [...header.lines, ...statusLines(rows)]
  const summary = requestSummaryLine(rows, alreadyTracked)
  if (rows.length + alreadyTracked > 1 && summary) {
    lines.push(`_${summary}_`)
  }
  if (closing) {
    lines.push(closing)
  }
  const text = lines.join('\n')
  if (header.buttons.length === 0) {
    return { text }
  }
  const sections = mrkdwnSections(text).slice(0, MAX_BLOCKS_PER_MESSAGE)
  const buttonBudget = Math.max(0, MAX_BLOCKS_PER_MESSAGE - sections.length) * BUTTONS_PER_BLOCK
  const buttons = header.buttons.slice(0, buttonBudget).map((button, index) => ({
    ...button,
    actionId: `${VIEW_INSIGHTS_PROJECT_ACTION_ID}:${index}`,
  }))
  return { text, blocks: [...sections, ...linkButtonBlocks(buttons)] }
}

export function initialRequestStatusClosing(evaluationStarted: boolean): string {
  return evaluationStarted
    ? CLOSING_LINE
    : `${STATUS.problem} I couldn't start the check right now, it will run with the next scheduled evaluation. ${CLOSING_LINE}`
}

export function buildAllDoneReply(rows: RequestStatusRow[], alreadyTracked = 0): string {
  return `${STATUS.added} All done: ${requestSummaryLine(rows, alreadyTracked)}.`
}

export async function refreshRequestStatusMessage(
  qx: QueryExecutor,
  request: RequestRef,
): Promise<boolean> {
  const sourceUrl = request.sourceUrl
  if (!sourceUrl) {
    return false
  }
  if (!(await findSlackOnboardingRequest(qx, sourceUrl))) {
    return false
  }

  return qx.tx(async (tx) => {
    await lockSlackOnboardingRequest(tx, sourceUrl)
    const status = await findSlackOnboardingRequest(tx, sourceUrl)
    if (!status) {
      return false
    }

    const rows = await findProjectCatalogBySourceUrl(tx, sourceUrl)
    const alreadyTracked = status.header.alreadyTracked ?? 0
    const content = buildRequestStatusContent({ header: status.header, rows, alreadyTracked })
    const updated = await updateSlackMessage({
      channel: status.channel,
      ts: status.messageTs,
      ...content,
    })
    if (!updated.ok) {
      log.warn(
        { id: request.id, repoUrl: request.repoUrl, error: updated.error },
        'Request status message was not updated.',
      )
      return false
    }

    if (rows.length + alreadyTracked > 1 && isRequestSettled(rows) && !status.doneNotifiedAt) {
      await announceAllDone(tx, sourceUrl, request, rows, alreadyTracked)
    }
    return true
  })
}

// Runs under the request lock, so the claim can safely follow the post: a failed post stays
// unclaimed and is retried by the next refresh.
async function announceAllDone(
  qx: QueryExecutor,
  sourceUrl: string,
  request: RequestRef,
  rows: RequestStatusRow[],
  alreadyTracked: number,
): Promise<void> {
  const posted = await postSlackContentInThread(sourceUrl, {
    text: buildAllDoneReply(rows, alreadyTracked),
  })
  if (!posted.ok) {
    log.warn({ id: request.id, error: posted.error }, 'All-done reply was not posted.')
    return
  }
  await claimSlackOnboardingRequestDone(qx, sourceUrl)
}

// Requests made before the status message existed, and rows without a Slack origin, keep
// getting one reply per outcome; everything else edits the status message in place.
export async function reportRequestOutcome(
  qx: QueryExecutor,
  request: RequestRef,
  fallback: string | ISlackMessageContent,
): Promise<{ ok: boolean; error?: string }> {
  if (!request.sourceUrl) {
    return { ok: false, error: 'request has no Slack origin' }
  }
  if (await refreshRequestStatusMessage(qx, request)) {
    return { ok: true }
  }
  const content = typeof fallback === 'string' ? { text: fallback } : fallback
  const posted = await postSlackContentInThread(request.sourceUrl, content)
  return posted.ok ? { ok: true } : { ok: false, error: posted.error }
}
