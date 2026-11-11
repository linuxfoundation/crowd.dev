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
  findTrackedRepoProjects,
  lockDiscussionOnboardingRequest,
  replaceDiscussionOnboardingRequestComment,
  setDiscussionOnboardingRequestNotes,
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
import { escapeGithubMarkdown } from './githubMarkdown'
import { findOnboardedInsightsProject } from './onboardedProjectLookup'
import { requestGroupReviewOf } from './organizationGroup'
import {
  MAX_LINE_REASON_LENGTH,
  RequestStatusRow,
  groupByRequestGroup,
  isRequestSettled,
  requestSummaryLine,
  rowStatus,
} from './requestStatus'

const log = getServiceLogger()

const TITLE = '**LFX Insights onboarding status**'
const CLOSING_LINE = '_Updated automatically as each repository is checked._'

type DiscussionRequestRef = Pick<IDbProjectCatalog, 'sourceUrl'> &
  Partial<Pick<IDbProjectCatalog, 'id' | 'repoUrl'>>

export interface IDiscussionRefreshOptions {
  organizationNotes?: string[]
}

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

function plainReason(reason: string): string {
  return escapeGithubMarkdown(truncateReason(reason, MAX_LINE_REASON_LENGTH))
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

function projectMarkdown(row: DiscussionStatusRow): string | null {
  if (!row.matchedProjectName) {
    return null
  }
  const name = escapeGithubMarkdown(row.matchedProjectName)
  return `**${row.insightsSlug ? `[${name}](${buildInsightsProjectUrl(row.insightsSlug)})` : name}**`
}

function addedDetail(row: DiscussionStatusRow): string {
  const project = projectMarkdown(row)
  return project ? `added to ${project}` : 'added'
}

function alreadyThereDetail(row: DiscussionStatusRow): string {
  const project = projectMarkdown(row)
  return project ? `already in LFX Insights, in ${project}` : 'already in LFX Insights'
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
      return alreadyThereDetail(row)
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

export function buildDiscussionStatusMarkdown(
  rows: DiscussionStatusRow[],
  notes: string[] = [],
): string {
  const lines = [TITLE]
  if (rows.length > 0) {
    lines.push('', ...groupByRequestGroup(rows).flatMap(groupLines))
  }
  if (notes.length > 0) {
    lines.push('', ...notes.map((note) => `- ${note}`))
  }
  const summary = requestSummaryLine(rows)
  if (rows.length > 1 && summary) {
    lines.push('', `_${summary}_`)
  }
  const pending = rows.length === 0 ? notes.length === 0 : !isRequestSettled(rows)
  if (pending) {
    lines.push('', CLOSING_LINE)
  }
  return lines.join('\n')
}

function sameNotes(stored: string[], notes: string[]): boolean {
  return stored.length === notes.length && stored.every((note, index) => note === notes[index])
}

export function buildDiscussionAllDoneMarkdown(rows: RequestStatusRow[]): string {
  return `${STATUS.added} All done: ${requestSummaryLine(rows)}.`
}

function isUneditableComment(err: unknown): boolean {
  return err instanceof GithubPublicClientError && (err.kind === 'NOT_FOUND' || err.kind === 'AUTH')
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
    if (!isUneditableComment(err)) {
      throw err
    }
    log.info(
      { commentId, error: getErrorMessage(err) },
      'Discussion status comment is gone or not ours to edit, posting a new one.',
    )
    return false
  }
}

async function writeStatusComment(
  qx: QueryExecutor,
  ref: IGithubDiscussionRef,
  sourceUrl: string,
  status: IDbDiscussionOnboardingRequest | null,
  body: string,
  notes: string[],
  client: IDiscussionCommentClient,
): Promise<IDbDiscussionOnboardingRequest> {
  if (!status) {
    const discussionId = await client.fetchDiscussionId(ref)
    const comment = await client.addComment(discussionId, body)
    return upsertDiscussionOnboardingRequest(qx, {
      sourceUrl,
      discussionId,
      commentId: comment.id,
      notes,
    })
  }
  let written = status
  if (!(await editComment(client, status.commentId, body))) {
    const replacement = await client.addComment(status.discussionId, body)
    written = await replaceDiscussionOnboardingRequestComment(qx, status.sourceUrl, replacement.id)
  }
  return sameNotes(status.notes, notes)
    ? written
    : setDiscussionOnboardingRequestNotes(qx, status.sourceUrl, notes)
}

type InsightsProjectLookup = Awaited<ReturnType<typeof findOnboardedInsightsProject>>

// A failed lookup rolls back to its savepoint, so the status update itself goes on.
async function lookupOrNull<T>(
  tx: QueryExecutor,
  context: Record<string, unknown>,
  lookup: (savepoint: QueryExecutor) => Promise<T>,
): Promise<T | null> {
  try {
    return await tx.tx(lookup)
  } catch (err) {
    log.warn(
      { ...context, error: getErrorMessage(err) },
      'Insights project lookup failed, naming the project from the catalog row.',
    )
    return null
  }
}

function isAlreadyTracked(row: IDbProjectCatalog): boolean {
  return row.action === 'skip' && row.classificationKind === 'already_tracked'
}

interface IInsightsProjectRef {
  name: string
  slug: string | null
}

async function trackedProjectsByRepoUrl(
  tx: QueryExecutor,
  rows: IDbProjectCatalog[],
): Promise<Map<string, IInsightsProjectRef>> {
  const repoUrls = rows.filter(isAlreadyTracked).map((row) => row.repoUrl)
  if (repoUrls.length === 0) {
    return new Map()
  }
  const tracked =
    (await lookupOrNull(tx, { repoUrls }, (savepoint) =>
      findTrackedRepoProjects(savepoint, repoUrls),
    )) ?? []
  return new Map(
    tracked.map((project) => [
      project.repoUrl,
      { name: project.projectName, slug: project.projectSlug },
    ]),
  )
}

async function withInsightsProjectNames(
  tx: QueryExecutor,
  rows: IDbProjectCatalog[],
): Promise<DiscussionStatusRow[]> {
  const projects = new Map<string, InsightsProjectLookup>()
  const tracked = await trackedProjectsByRepoUrl(tx, rows)
  const named: DiscussionStatusRow[] = []
  for (const row of rows) {
    let project: IInsightsProjectRef | null = null
    if (row.action === 'onboarded' && row.matchedSegmentId) {
      if (!projects.has(row.matchedSegmentId)) {
        projects.set(
          row.matchedSegmentId,
          await lookupOrNull(tx, { id: row.id, repoUrl: row.repoUrl }, (savepoint) =>
            findOnboardedInsightsProject(savepoint, row),
          ),
        )
      }
      project = projects.get(row.matchedSegmentId) ?? null
    } else if (isAlreadyTracked(row)) {
      project = tracked.get(row.repoUrl) ?? null
    }
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
  options: IDiscussionRefreshOptions = {},
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
      const status = await findDiscussionOnboardingRequest(tx, sourceUrl)
      const storedNotes = status?.notes ?? []
      const notes = options.organizationNotes ?? storedNotes
      const notesChanged = !sameNotes(storedNotes, notes)
      if (options.organizationNotes && !notesChanged) {
        return false
      }
      const catalogRows = await findProjectCatalogBySourceUrl(tx, sourceUrl)
      if (catalogRows.length === 0 && notes.length === 0 && !notesChanged) {
        return false
      }
      const rows = await withInsightsProjectNames(tx, catalogRows)
      const written = await writeStatusComment(
        tx,
        ref,
        sourceUrl,
        status,
        buildDiscussionStatusMarkdown(rows, notes),
        notes,
        commentClient,
      )
      if (rows.length > 1 && isRequestSettled(rows)) {
        await announceAllDone(tx, written, rows, commentClient)
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
