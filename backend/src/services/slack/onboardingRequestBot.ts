import {
  MAX_REQUEST_TEXT_LENGTH,
  extractGithubRepoUrls,
  fetchPublicOrgRepos,
  getErrorMessage,
  getGithubToken,
  isPublicGithubRepo,
  truncateRequestText,
} from '@crowd/common'
import {
  bulkInsertProjectCatalog,
  deriveProjectIdentityFromRepoUrl,
  findExistingProjectCatalogRepoUrls,
  findProjectCatalogByRepoUrls,
  findTrackedRepoProjects,
  promoteProjectCatalogProvenance,
  reopenRejectedProjectCatalog,
  upsertSlackOnboardingRequest,
} from '@crowd/data-access-layer'
import {
  IDbProjectCatalog,
  IDbProjectCatalogCreate,
  IDbProjectCatalogReopened,
} from '@crowd/data-access-layer/src/project-catalog/types'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import {
  BotStatus,
  IExpandedOrganization,
  IParsedOnboardingRequest,
  OrganizationRepoLookup,
  REQUESTER_ERROR_DECISION_REASONS,
  RepositoryLookup,
  STATUS_REACTION,
  alreadyRequestedStatus,
  buildBotFailureReply,
  buildBotHelpReply,
  buildBotHierarchyQuestionReply,
  buildBotIngestReply,
  buildBotNoRepositoryReply,
  buildIngestStatusHeader,
  checkRepositoriesOnGithub,
  collectIngestNotes,
  expandOrganizations,
  missingRepositoryLine,
  parseOnboardingRequest,
  reopenedRequestNote,
  requestedOrganizations,
} from '@crowd/project-onboarding'
import { createOnboardingRequestLlm } from '@crowd/project-onboarding/src/requestLlm'
import {
  ISlackMessageContent,
  addSlackReaction,
  getSlackPermalink,
  postSlackMessage,
  updateSlackMessage,
} from '@crowd/slack'
import { type Client as TemporalClient, WorkflowIdReusePolicy } from '@crowd/temporal'

import { createDailyLlmCap } from '../../api/public/v1/projectEvaluation/dailyLlmCap'
import { IServiceOptions } from '../IServiceOptions'
import { getBgQx } from './slackBackground'

const MENTION_PATTERN = /<@[A-Z0-9]+>/g
const LINK_WITH_LABEL_PATTERN = /<(https?:\/\/[^|>\s]+)\|[^>]*>/g
const LINK_PATTERN = /<(https?:\/\/[^>\s]+)>/g
const HELP_KEYWORD = 'help'

const reserveDailyLlmCall = createDailyLlmCap()

const lookupOrganizationRepos: OrganizationRepoLookup = (owner, limit) =>
  fetchPublicOrgRepos(owner, getGithubToken(), limit)

const GITHUB_REPO_CHECK_TIMEOUT_MS = 10_000

const lookupRepository: RepositoryLookup = (repoUrl) =>
  isPublicGithubRepo(repoUrl, getGithubToken(), AbortSignal.timeout(GITHUB_REPO_CHECK_TIMEOUT_MS))

export interface IOnboardingRequestIngest {
  requestText: string
  repoUrls: string[]
  sourceUrl: string
  projectName: string | null
  requestedBy?: string | null
  organizations?: IExpandedOrganization[]
}

export interface IOnboardingRequestIngestResult {
  queued: string[]
  queuedIds: string[]
  alreadyCatalogued: string[]
  reopened: IDbProjectCatalogReopened[]
}

export interface IOnboardingRequestEvaluation {
  channelId: string
  messageTs: string
  catalogIds: string[]
}

export type OnboardingRequestTemporal = Pick<TemporalClient, 'workflow'>

type RequestLog = Pick<IServiceOptions, 'log'>['log']

export function toRequestText(slackText: string): string {
  return slackText
    .replace(MENTION_PATTERN, '')
    .replace(LINK_WITH_LABEL_PATTERN, '$1')
    .replace(LINK_PATTERN, '$1')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&')
    .trim()
}

function isHelpRequest(requestText: string): boolean {
  return requestText.toLowerCase() === HELP_KEYWORD
}

function organizationByRepoUrl(organizations: IExpandedOrganization[]): Map<string, string> {
  return new Map(
    organizations.flatMap((org) => org.repoUrls.map((repoUrl) => [repoUrl, org.owner] as const)),
  )
}

function toCatalogRow(
  repoUrl: string,
  { requestText, sourceUrl, projectName, requestedBy }: Omit<IOnboardingRequestIngest, 'repoUrls'>,
  organization: string | undefined,
): IDbProjectCatalogCreate | null {
  const identity = deriveProjectIdentityFromRepoUrl(repoUrl)
  if (!identity) {
    return null
  }

  return {
    ...identity,
    repoUrl,
    source: 'slack-bot',
    sourceUrl,
    provenance: 'slack-bot',
    action: 'auto',
    requestText,
    projectName,
    requestedBy: requestedBy ?? null,
    requestHints: organization ? { organization } : null,
  }
}

export async function ingestOnboardingRequest(
  qx: QueryExecutor,
  {
    requestText,
    repoUrls,
    sourceUrl,
    projectName,
    requestedBy,
    organizations = [],
  }: IOnboardingRequestIngest,
): Promise<IOnboardingRequestIngestResult> {
  const existing = await findExistingProjectCatalogRepoUrls(qx, repoUrls)
  const owners = organizationByRepoUrl(organizations)
  const request = { requestText, sourceUrl, projectName, requestedBy }
  const reopened = await reopenRejectedProjectCatalog(
    qx,
    [...existing].map((repoUrl) => ({
      ...request,
      repoUrl,
      requestedBy: requestedBy ?? null,
      organization: owners.get(repoUrl) ?? null,
    })),
    REQUESTER_ERROR_DECISION_REASONS,
  )
  const rows = repoUrls
    .filter((repoUrl) => !existing.has(repoUrl))
    .map((repoUrl) => toCatalogRow(repoUrl, request, owners.get(repoUrl)))
    .filter((row): row is IDbProjectCatalogCreate => row !== null)

  const inserted = await bulkInsertProjectCatalog(qx, rows)
  const queuedIdByUrl = new Map([...inserted, ...reopened].map((row) => [row.repoUrl, row.id]))
  const queued = repoUrls.filter((repoUrl) => queuedIdByUrl.has(repoUrl))
  const alreadyCatalogued = repoUrls.filter((repoUrl) => !queuedIdByUrl.has(repoUrl))

  await promoteProjectCatalogProvenance(
    qx,
    'slack-bot',
    alreadyCatalogued.map((repoUrl) => {
      const organization = owners.get(repoUrl)
      return organization
        ? { repoUrl, sourceUrl, organization, requestText, projectName, requestedBy }
        : { repoUrl, sourceUrl }
    }),
  )

  return {
    queued,
    queuedIds: queued.map((repoUrl) => queuedIdByUrl.get(repoUrl) as string),
    alreadyCatalogued,
    reopened,
  }
}

export async function startOnboardingRequestEvaluation(
  temporal: OnboardingRequestTemporal,
  { channelId, messageTs, catalogIds }: IOnboardingRequestEvaluation,
): Promise<void> {
  try {
    await temporal.workflow.start('evaluateProjects', {
      taskQueue: 'projects-evaluation',
      workflowId: `evaluate-slack-${channelId}-${messageTs}`,
      workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
      args: [{ catalogIds }],
    })
  } catch (err) {
    if (err instanceof Error && err.name === 'WorkflowExecutionAlreadyStartedError') {
      return
    }
    throw err
  }
}

async function startEvaluationOrWarn(
  temporal: OnboardingRequestTemporal | null,
  evaluation: IOnboardingRequestEvaluation,
  threadTs: string,
  log: RequestLog,
): Promise<boolean> {
  const { channelId, catalogIds } = evaluation
  if (!temporal) {
    log.warn(
      { channelId, threadTs, catalogIds },
      'Temporal is not configured, the Slack request waits for the scheduled evaluation.',
    )
    return false
  }
  try {
    await startOnboardingRequestEvaluation(temporal, evaluation)
    return true
  } catch (err) {
    log.error(
      { error: getErrorMessage(err), channelId, threadTs, catalogIds },
      'Immediate evaluation could not be started for the Slack request.',
    )
    return false
  }
}

function requestedRepoUrls(
  requestText: string,
  parsed: IParsedOnboardingRequest | null,
  organizations: IExpandedOrganization[],
): string[] {
  const listed = parsed ? parsed.githubRepoUrls : extractGithubRepoUrls(requestText)
  return [...new Set([...listed, ...organizations.flatMap((org) => org.repoUrls)])]
}

function reserveParserCall(
  actorId: string | undefined,
  log: RequestLog,
  context: { channelId: string; threadTs: string },
): boolean {
  if (!actorId) {
    log.warn(
      context,
      'Slack mention without a user id, falling back to the GitHub links in the text.',
    )
    return false
  }
  try {
    reserveDailyLlmCall(actorId)
    return true
  } catch (err) {
    log.warn(
      { ...context, actorId, error: getErrorMessage(err) },
      'Daily parser limit reached for this Slack user, falling back to the GitHub links in the text.',
    )
    return false
  }
}

interface IEarlierRequests {
  rows: IDbProjectCatalog[]
  adoptedRepoUrls: Set<string>
}

// Rows the ingest just attached to this request (their origin is now this message) are
// rendered from the catalog by the status message; the others stay described as earlier requests.
async function earlierRequests(
  qx: QueryExecutor,
  repoUrls: string[],
  knownBefore: IDbProjectCatalog[],
  sourceUrl: string,
  log: RequestLog,
): Promise<IEarlierRequests> {
  const originBefore = new Map(knownBefore.map((row) => [row.repoUrl, row.sourceUrl]))
  let fresh = knownBefore.filter((row) => repoUrls.includes(row.repoUrl))
  try {
    fresh = await findProjectCatalogByRepoUrls(qx, repoUrls)
  } catch (err) {
    log.warn(
      { repoUrls, error: getErrorMessage(err) },
      'Earlier requests could not be refreshed, answering from the pre-ingest snapshot.',
    )
  }
  const adoptedRepoUrls = new Set(
    fresh
      .filter((row) => row.sourceUrl === sourceUrl && originBefore.get(row.repoUrl) !== sourceUrl)
      .map((row) => row.repoUrl),
  )
  const rows = fresh.map((row) =>
    originBefore.has(row.repoUrl)
      ? { ...row, sourceUrl: originBefore.get(row.repoUrl) ?? null }
      : row,
  )
  return { rows, adoptedRepoUrls }
}

export async function parseRequestOrFallBack(
  qx: QueryExecutor,
  requestText: string,
  actorId: string | undefined,
  log: RequestLog,
  context: { channelId: string; threadTs: string },
): Promise<IParsedOnboardingRequest | null> {
  if (!reserveParserCall(actorId, log, context)) {
    return null
  }
  try {
    const parsed = await parseOnboardingRequest(requestText, createOnboardingRequestLlm(qx))
    if (parsed.ok === false) {
      log.warn(
        { ...context, reason: parsed.reason },
        'Onboarding request could not be parsed, falling back to the GitHub links in the text.',
      )
      return null
    }
    return parsed.request
  } catch (err) {
    log.warn(
      { ...context, error: getErrorMessage(err) },
      'Onboarding request parser failed, falling back to the GitHub links in the text.',
    )
  }
  return null
}

export async function runOnboardingRequestBot({
  text,
  channelId,
  messageTs,
  threadTs,
  actorId,
  options,
}: {
  text: string
  channelId: string
  messageTs: string
  threadTs: string
  actorId?: string
  options: Pick<IServiceOptions, 'log'> & { temporal: OnboardingRequestTemporal | null }
}): Promise<void> {
  const { log, temporal } = options
  const reply = async (message: string | ISlackMessageContent): Promise<string | null> => {
    const content = typeof message === 'string' ? { text: message } : message
    const result = await postSlackMessage({
      channel: channelId,
      thread_ts: threadTs,
      ...content,
    })
    if (!result.ok) {
      log.warn({ channelId, threadTs, error: result.error }, 'Slack bot reply was not delivered.')
      return null
    }
    return result.ts ?? null
  }
  const rememberStatusMessage = async (
    qx: QueryExecutor,
    sourceUrl: string,
    statusTs: string | null,
    header: ReturnType<typeof buildIngestStatusHeader>,
  ) => {
    if (!statusTs) {
      return
    }
    try {
      await upsertSlackOnboardingRequest(qx, {
        sourceUrl,
        channel: channelId,
        messageTs: statusTs,
        header,
      })
    } catch (err) {
      log.warn(
        { channelId, threadTs, error: getErrorMessage(err) },
        'Status message of the request was not remembered, outcomes will be posted as replies.',
      )
    }
  }
  const react = async (status: BotStatus) => {
    const result = await addSlackReaction({
      channel: channelId,
      timestamp: messageTs,
      name: STATUS_REACTION[status],
    })
    if (!result.ok) {
      log.warn(
        { channelId, messageTs, status, error: result.error },
        'Slack reaction was not added.',
      )
    }
  }

  const fullText = toRequestText(text)
  if (!fullText || isHelpRequest(fullText)) {
    await reply(buildBotHelpReply())
    return
  }
  const truncated = fullText.length > MAX_REQUEST_TEXT_LENGTH
  const requestText = truncateRequestText(fullText)

  try {
    const qx = await getBgQx()
    const parsed = await parseRequestOrFallBack(qx, requestText, actorId, log, {
      channelId,
      threadTs,
    })
    if (parsed?.asksAboutHierarchy) {
      await reply(buildBotHierarchyQuestionReply(parsed))
      return
    }

    const owners = requestedOrganizations(requestText, parsed)
    let organizations: IExpandedOrganization[] = []
    if (owners.length > 0) {
      const expansion = await expandOrganizations(owners, lookupOrganizationRepos)
      if (expansion.ok === false) {
        log.info(
          { channelId, threadTs, owners, reason: expansion.reason },
          'Organization request was not expanded.',
        )
        await reply(expansion.reply)
        return
      }
      organizations = expansion.organizations
    }

    const listedRepoUrls = requestedRepoUrls(requestText, parsed, organizations)
    if (listedRepoUrls.length === 0) {
      await reply(buildBotNoRepositoryReply(parsed, { truncated, requestText }))
      return
    }

    const alreadyTracked = await findTrackedRepoProjects(qx, listedRepoUrls)
    const trackedUrls = new Set(alreadyTracked.map((project) => project.repoUrl))
    const fromOrganizations = new Set(organizations.flatMap((org) => org.repoUrls))
    const check = await checkRepositoriesOnGithub(
      listedRepoUrls.filter(
        (repoUrl) => !trackedUrls.has(repoUrl) && !fromOrganizations.has(repoUrl),
      ),
      lookupRepository,
    )
    for (const { repoUrl, reason } of check.unchecked) {
      log.warn(
        { channelId, threadTs, repoUrl, reason },
        'Repository could not be checked on GitHub.',
      )
    }
    const missingUrls = new Set(check.missing)
    const missingLines = check.missing.map(missingRepositoryLine)
    const repoUrls = listedRepoUrls.filter((repoUrl) => !missingUrls.has(repoUrl))
    if (missingUrls.size > 0) {
      await react('notAdded')
    }
    if (repoUrls.length === 0) {
      await reply(missingLines.join('\n'))
      return
    }

    const requested = repoUrls.filter((repoUrl) => !trackedUrls.has(repoUrl))
    const notes = [
      ...missingLines,
      ...collectIngestNotes({
        requestText,
        repoUrls,
        nonGithubRepoUrls: parsed ? parsed.nonGithubRepoUrls : null,
        truncated,
        organizations,
      }),
    ]
    if (requested.length === 0) {
      log.info(
        { channelId, threadTs, tracked: alreadyTracked.length },
        'Every requested repository is already tracked.',
      )
      await reply(
        buildBotIngestReply({ queued: [], alreadyRequested: [], alreadyTracked, notes }, true),
      )
      await react('alreadyThere')
      return
    }

    const sourceUrl = await getSlackPermalink(channelId, messageTs)
    if (!sourceUrl) {
      throw new Error('Slack permalink for the request message is unavailable.')
    }
    const knownBefore = await findProjectCatalogByRepoUrls(qx, requested)
    const result = await ingestOnboardingRequest(qx, {
      requestText,
      repoUrls: requested,
      sourceUrl,
      projectName: parsed?.projectName ?? null,
      requestedBy: actorId ?? null,
      organizations,
    })

    log.info(
      {
        channelId,
        threadTs,
        queued: result.queued.length,
        reopened: result.reopened.length,
        alreadyCatalogued: result.alreadyCatalogued.length,
      },
      'Onboarding request queued from Slack.',
    )

    const { rows: alreadyRequested, adoptedRepoUrls } = await earlierRequests(
      qx,
      result.alreadyCatalogued,
      knownBefore,
      sourceUrl,
      log,
    )
    const outcome = {
      queued: result.queued,
      alreadyRequested,
      alreadyTracked,
      notes: [...notes, ...result.reopened.map(reopenedRequestNote)],
    }
    // The status message must exist before the evaluation runs, or a fast outcome has nowhere to land.
    const statusTs = await reply(buildBotIngestReply(outcome, true))
    if (result.queuedIds.length > 0 || adoptedRepoUrls.size > 0) {
      await rememberStatusMessage(
        qx,
        sourceUrl,
        statusTs,
        buildIngestStatusHeader(outcome, adoptedRepoUrls),
      )
    }

    const evaluationStarted =
      result.queuedIds.length === 0 ||
      (await startEvaluationOrWarn(
        temporal,
        { channelId, messageTs, catalogIds: result.queuedIds },
        threadTs,
        log,
      ))
    if (!evaluationStarted && statusTs) {
      const corrected = await updateSlackMessage({
        channel: channelId,
        ts: statusTs,
        ...buildBotIngestReply(outcome, false),
      })
      if (!corrected.ok) {
        log.warn(
          { channelId, threadTs, error: corrected.error },
          'Status message was not corrected.',
        )
      }
    }
    if (alreadyTracked.length > 0) {
      await react('alreadyThere')
    }
    for (const status of new Set(alreadyRequested.map(alreadyRequestedStatus))) {
      await react(status)
    }
    if (result.queuedIds.length > 0) {
      await react('checking')
    }
  } catch (err) {
    log.error({ error: getErrorMessage(err), channelId, threadTs }, 'Slack request failed.')
    await reply(buildBotFailureReply())
    await react('problem')
  }
}
