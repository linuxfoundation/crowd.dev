import {
  GithubPublicClientError,
  IGithubDiscussionComment,
  IGithubDiscussionRef,
  addGithubDiscussionComment,
  canonicalizeRepoUrl,
  fetchGithubDiscussionId,
  getErrorMessage,
  getGithubDiscussionsToken,
  parseGithubDiscussionUrl,
  updateGithubDiscussionComment,
} from '@crowd/common'
import {
  IDbDiscussionOnboardingRequest,
  claimDiscussionOnboardingRequestDone,
  findDiscussionOnboardingRequest,
  findProjectCatalogBySourceUrl,
  lockDiscussionOnboardingRequest,
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

export interface IDiscussionCommentClient {
  fetchDiscussionId: (ref: IGithubDiscussionRef) => Promise<string>
  addComment: (discussionId: string, body: string) => Promise<IGithubDiscussionComment>
  updateComment: (commentId: string, body: string) => Promise<IGithubDiscussionComment>
}

function tokenClient(): IDiscussionCommentClient {
  const token = getGithubDiscussionsToken()
  return {
    fetchDiscussionId: (ref) => fetchGithubDiscussionId(ref, token),
    addComment: (discussionId, body) => addGithubDiscussionComment(discussionId, body, token),
    updateComment: (commentId, body) => updateGithubDiscussionComment(commentId, body, token),
  }
}

function plainReason(reason: string): string {
  const trimmed = reason.trim().replace(/[. ]+$/, '')
  return trimmed.length > MAX_REASON_LENGTH
    ? `${trimmed.slice(0, MAX_REASON_LENGTH).trimEnd()}…`
    : trimmed
}

function repoMarkdown(repoUrl: string): string {
  const canonical = canonicalizeRepoUrl(repoUrl)
  return canonical?.isGithub ? `[${canonical.owner}/${canonical.repo}](${canonical.url})` : repoUrl
}

function notAddedDetail(row: RequestStatusRow): string {
  if (row.reviewedBy && isReviewRejectionSkipReason(row.skipReason)) {
    const reason = describeDecisionReason(row.decisionReason)
    return reason
      ? `the CDP team reviewed it: ${reason}`
      : 'the CDP team reviewed it and rejected the request'
  }
  return plainReason(row.skipReason ?? row.evaluationReason ?? 'no reason was recorded')
}

function statusDetail(status: BotStatus, row: RequestStatusRow): string {
  switch (status) {
    case 'withTeam':
      return `with the CDP team${row.reviewReason ? `: ${plainReason(row.reviewReason)}` : ''}`
    case 'needsInput':
      return 'waiting for input'
    case 'added':
      return `added${row.matchedProjectName ? ` to **${row.matchedProjectName}**` : ''}`
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

function rowLine(row: RequestStatusRow, indent = ''): string {
  const status = rowStatus(row)
  return `${indent}- ${STATUS[status]} ${repoMarkdown(row.repoUrl)} · ${statusDetail(status, row)}`
}

function sharedDetail(rows: RequestStatusRow[]): string | null {
  const statuses = new Set(rows.map(rowStatus))
  if (statuses.size !== 1) {
    return null
  }
  const [status] = statuses
  const details = new Set(rows.map((row) => statusDetail(status, row)))
  return details.size === 1 ? `${STATUS[status]} ${[...details][0]}` : null
}

function groupLines(rows: RequestStatusRow[]): string[] {
  const review = requestGroupReviewOf(rows[0], rows.length - 1)
  if (!review) {
    return rows.map((row) => rowLine(row))
  }
  const shared = sharedDetail(rows)
  const heading = `- **${reviewGroupLabel(review)}** · ${rows.length} repositories · ${shared ?? requestSummaryLine(rows)}`
  const members = rows.map((row) =>
    shared ? `  - ${repoMarkdown(row.repoUrl)}` : rowLine(row, '  '),
  )
  return [heading, ...members]
}

export function buildDiscussionStatusMarkdown(rows: RequestStatusRow[]): string {
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
  const discussionId = status?.discussionId ?? (await client.fetchDiscussionId(ref))
  const comment = await client.addComment(discussionId, body)
  return upsertDiscussionOnboardingRequest(qx, { sourceUrl, discussionId, commentId: comment.id })
}

// Runs under the request lock, so the claim can safely follow the post: a failed post stays
// unclaimed and is retried by the next refresh.
async function announceAllDone(
  qx: QueryExecutor,
  status: IDbDiscussionOnboardingRequest,
  rows: RequestStatusRow[],
  client: IDiscussionCommentClient,
): Promise<void> {
  if (status.doneNotifiedAt) {
    return
  }
  await client.addComment(status.discussionId, buildDiscussionAllDoneMarkdown(rows))
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
      const rows = await findProjectCatalogBySourceUrl(tx, sourceUrl)
      if (rows.length === 0) {
        return false
      }
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
    log.warn(
      { id: request.id, repoUrl: request.repoUrl, sourceUrl, error: getErrorMessage(err) },
      'Discussion status comment was not updated.',
    )
    return false
  }
}
