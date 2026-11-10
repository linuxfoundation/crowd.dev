import {
  GithubPublicClientError,
  IGithubDiscussionComment,
  IGithubDiscussionRef,
  addGithubDiscussionComment,
  canonicalizeRepoUrl,
  fetchGithubDiscussionId,
  getErrorMessage,
  parseGithubDiscussionUrl,
  updateGithubDiscussionComment,
} from '@crowd/common'
import {
  IDbDiscussionOnboardingRequest,
  claimDiscussionOnboardingRequestDone,
  findDiscussionOnboardingRequest,
  findProjectCatalogBySourceUrl,
  lockDiscussionOnboardingRequest,
  replaceDiscussionOnboardingRequestComment,
  upsertDiscussionOnboardingRequest,
} from '@crowd/data-access-layer'
import {
  IDbProjectCatalog,
  isReviewRejectionSkipReason,
} from '@crowd/data-access-layer/src/project-catalog/types'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import { describeDecisionReason, reviewGroupLabel } from '@crowd/slack'

import { BotStatus, STATUS } from './botCopy'
import { buildInsightsProjectUrl, truncateReason } from './botThreadReply'
import { resolveDiscussionToken } from './discussionToken'
import { findOnboardedInsightsProject } from './onboardedProjectLookup'
import { requestGroupReviewOf } from './organizationGroup'
import {
  RequestStatusRow,
  groupByRequestGroup,
  isRequestSettled,
  requestSummaryLine,
  rowStatus,
} from './requestStatus'

const log = getServiceLogger()

const TITLE = '**LFX Insights onboarding status**'
const CLOSING_LINE = '_Updated automatically as each repository is checked._'
const MAX_REASON_LENGTH = 160

type DiscussionRequestRef = Pick<IDbProjectCatalog, 'id' | 'repoUrl' | 'sourceUrl'>

export type DiscussionStatusRow = RequestStatusRow & { insightsSlug?: string | null }

export interface IDiscussionCommentClient {
  fetchDiscussionId: (ref: IGithubDiscussionRef) => Promise<string>
  addComment: (discussionId: string, body: string) => Promise<IGithubDiscussionComment>
  updateComment: (commentId: string, body: string) => Promise<IGithubDiscussionComment>
}

function tokenClient(): IDiscussionCommentClient {
  return {
    fetchDiscussionId: async (ref) => fetchGithubDiscussionId(ref, await resolveDiscussionToken()),
    addComment: async (discussionId, body) =>
      addGithubDiscussionComment(discussionId, body, await resolveDiscussionToken()),
    updateComment: async (commentId, body) =>
      updateGithubDiscussionComment(commentId, body, await resolveDiscussionToken()),
  }
}

function escapeGithubMarkdown(text: string): string {
  return text.replace(/[\\`*_[\]<>#~]/g, '\\$&').replace(/@/g, '&#64;')
}

function plainReason(reason: string): string {
  return escapeGithubMarkdown(truncateReason(reason, MAX_REASON_LENGTH))
}

function repoMarkdown(repoUrl: string): string {
  const canonical = canonicalizeRepoUrl(repoUrl)
  return canonical?.isGithub ? `[${canonical.owner}/${canonical.repo}](${canonical.url})` : repoUrl
}

function reviewNote(note: string | null): string {
  const trimmed = note
    ?.split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .join(' ')
  return trimmed ? ` — ${escapeGithubMarkdown(trimmed)}` : ''
}

function notAddedDetail(row: DiscussionStatusRow): string {
  if (row.reviewedBy && isReviewRejectionSkipReason(row.skipReason)) {
    const reason = describeDecisionReason(row.decisionReason)
    const verdict = reason
      ? `the CDP team reviewed it: ${reason}`
      : 'the CDP team reviewed it and rejected the request'
    return `${verdict}${reviewNote(row.decisionNote)}`
  }
  return plainReason(row.skipReason ?? row.evaluationReason ?? 'no reason was recorded')
}

function addedDetail(row: DiscussionStatusRow): string {
  if (!row.matchedProjectName) {
    return 'added'
  }
  const name = escapeGithubMarkdown(row.matchedProjectName)
  const project = row.insightsSlug
    ? `[${name}](${buildInsightsProjectUrl(row.insightsSlug)})`
    : name
  return `added to **${project}**`
}

function statusDetail(status: BotStatus, row: DiscussionStatusRow): string {
  switch (status) {
    case 'withTeam':
      return `with the CDP team${row.reviewReason ? `: ${plainReason(row.reviewReason)}` : ''}`
    case 'needsInput':
      return 'waiting for input'
    case 'added':
      return addedDetail(row)
    case 'alreadyThere':
      return 'already in LFX Insights'
    case 'notAdded':
      return `not added: ${notAddedDetail(row)}`
    case 'problem':
      return 'something went wrong on our side, the CDP team has been alerted'
    default:
      return 'checking…'
  }
}

function rowLine(row: DiscussionStatusRow, indent = ''): string {
  const status = rowStatus(row)
  return `${indent}- ${STATUS[status]} ${repoMarkdown(row.repoUrl)} · ${statusDetail(status, row)}`
}

function sharedDetail(rows: DiscussionStatusRow[]): string | null {
  const statuses = new Set(rows.map(rowStatus))
  if (statuses.size !== 1) {
    return null
  }
  const [status] = statuses
  const details = new Set(rows.map((row) => statusDetail(status, row)))
  return details.size === 1 ? `${STATUS[status]} ${[...details][0]}` : null
}

function groupLines(rows: DiscussionStatusRow[]): string[] {
  const review = requestGroupReviewOf(rows[0], rows.length - 1)
  if (!review) {
    return rows.map((row) => rowLine(row))
  }
  const shared = sharedDetail(rows)
  const heading = `- **${escapeGithubMarkdown(reviewGroupLabel(review))}** · ${rows.length} repositories · ${shared ?? requestSummaryLine(rows)}`
  const members = rows.map((row) =>
    shared ? `  - ${repoMarkdown(row.repoUrl)}` : rowLine(row, '  '),
  )
  return [heading, ...members]
}

export function buildDiscussionStatusMarkdown(rows: DiscussionStatusRow[]): string {
  const lines = [TITLE, '', ...groupByRequestGroup(rows).flatMap(groupLines)]
  const summary = requestSummaryLine(rows)
  if (rows.length > 1 && summary) {
    lines.push('', `_${summary}_`)
  }
  if (!isRequestSettled(rows)) {
    lines.push('', CLOSING_LINE)
  }
  return lines.join('\n')
}

export function buildDiscussionAllDoneMarkdown(rows: RequestStatusRow[]): string {
  return `${STATUS.added} All done: ${requestSummaryLine(rows)}.`
}

function isMissingComment(err: unknown): boolean {
  return err instanceof GithubPublicClientError && err.kind === 'NOT_FOUND'
}

function isSetupError(err: unknown): boolean {
  return (
    err instanceof GithubPublicClientError &&
    (err.kind === 'CONFIG' || err.kind === 'AUTH' || err.kind === 'ACCESS_DENIED')
  )
}

async function editComment(
  client: IDiscussionCommentClient,
  commentId: string,
  body: string,
): Promise<boolean> {
  try {
    await client.updateComment(commentId, body)
    return true
  } catch (err) {
    if (!isMissingComment(err)) {
      throw err
    }
    log.info({ commentId }, 'Discussion status comment is gone, posting a new one.')
    return false
  }
}

async function writeStatusComment(
  qx: QueryExecutor,
  ref: IGithubDiscussionRef,
  sourceUrl: string,
  body: string,
  client: IDiscussionCommentClient,
): Promise<IDbDiscussionOnboardingRequest> {
  const status = await findDiscussionOnboardingRequest(qx, sourceUrl)
  if (status && (await editComment(client, status.commentId, body))) {
    return status
  }
  if (status) {
    const replacement = await client.addComment(status.discussionId, body)
    return replaceDiscussionOnboardingRequestComment(qx, sourceUrl, replacement.id)
  }
  const discussionId = await client.fetchDiscussionId(ref)
  const comment = await client.addComment(discussionId, body)
  return upsertDiscussionOnboardingRequest(qx, { sourceUrl, discussionId, commentId: comment.id })
}

type InsightsProjectLookup = Awaited<ReturnType<typeof findOnboardedInsightsProject>>

async function lookupInsightsProject(
  qx: QueryExecutor,
  row: IDbProjectCatalog,
): Promise<InsightsProjectLookup> {
  try {
    return await findOnboardedInsightsProject(qx, row)
  } catch (err) {
    log.warn(
      { id: row.id, repoUrl: row.repoUrl, error: getErrorMessage(err) },
      'Insights project lookup failed, naming the project from the catalog row.',
    )
    return null
  }
}

async function withInsightsProjectNames(
  qx: QueryExecutor,
  rows: IDbProjectCatalog[],
): Promise<DiscussionStatusRow[]> {
  const projects = new Map<string, InsightsProjectLookup>()
  const named: DiscussionStatusRow[] = []
  for (const row of rows) {
    if (row.action !== 'onboarded' || !row.matchedSegmentId) {
      named.push(row)
      continue
    }
    if (!projects.has(row.matchedSegmentId)) {
      projects.set(row.matchedSegmentId, await lookupInsightsProject(qx, row))
    }
    const project = projects.get(row.matchedSegmentId) ?? null
    named.push({
      ...row,
      matchedProjectName: row.matchedProjectName ?? project?.name ?? null,
      insightsSlug: project?.slug ?? null,
    })
  }
  return named
}

// The status comment is already stored when this runs, so a failed "all done" post only logs:
// the done claim stays open and the next refresh posts it again, without a second status comment.
async function announceAllDone(
  qx: QueryExecutor,
  status: IDbDiscussionOnboardingRequest,
  rows: DiscussionStatusRow[],
  client: IDiscussionCommentClient,
): Promise<void> {
  if (status.doneNotifiedAt) {
    return
  }
  try {
    await client.addComment(status.discussionId, buildDiscussionAllDoneMarkdown(rows))
  } catch (err) {
    log.warn(
      { sourceUrl: status.sourceUrl, error: getErrorMessage(err) },
      'All-done comment was not posted on the discussion.',
    )
    return
  }
  await claimDiscussionOnboardingRequestDone(qx, status.sourceUrl)
}

export async function refreshDiscussionStatusComment(
  qx: QueryExecutor,
  request: DiscussionRequestRef,
  client: IDiscussionCommentClient | null = null,
): Promise<boolean> {
  const sourceUrl = request.sourceUrl
  const ref = sourceUrl ? parseGithubDiscussionUrl(sourceUrl) : null
  if (!sourceUrl || !ref) {
    return false
  }

  try {
    const commentClient = client ?? tokenClient()
    return await qx.tx(async (tx) => {
      await lockDiscussionOnboardingRequest(tx, sourceUrl)
      const catalogRows = await findProjectCatalogBySourceUrl(tx, sourceUrl)
      if (catalogRows.length === 0) {
        return false
      }
      const rows = await withInsightsProjectNames(qx, catalogRows)
      const status = await writeStatusComment(
        tx,
        ref,
        sourceUrl,
        buildDiscussionStatusMarkdown(rows),
        commentClient,
      )
      if (rows.length > 1 && isRequestSettled(rows)) {
        await announceAllDone(tx, status, rows, commentClient)
      }
      return true
    })
  } catch (err) {
    const context = {
      id: request.id,
      repoUrl: request.repoUrl,
      sourceUrl,
      error: getErrorMessage(err),
    }
    if (isSetupError(err)) {
      log.error(
        context,
        'Discussion status comment was not updated: GitHub token is missing, rejected or denied access.',
      )
    } else {
      log.warn(context, 'Discussion status comment was not updated.')
    }
    return false
  }
}
