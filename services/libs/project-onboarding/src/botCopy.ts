import { canonicalizeRepoUrl } from '@crowd/common'
import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'
import { ReviewCardAction, escapeSlackText } from '@crowd/slack'

export const INSIGHTS_PROJECT_URL_BASE = 'https://insights.linuxfoundation.org/project'
export const VIEW_INSIGHTS_PROJECT_ACTION_ID = 'view_insights_project'
export const RETRY_EVALUATION_ACTION_ID = 'retry_evaluation'
export const ANSWER_REQUEST_ACTION_ID = 'answer_request'

export const STATUS = {
  checking: '⏳',
  added: '✅',
  alreadyThere: '📘',
  withTeam: '👀',
  needsInput: '❓',
  notAdded: '🚫',
  problem: '⚠️',
} as const

export type BotStatus = keyof typeof STATUS

export const STATUS_REACTION: Record<BotStatus, string> = {
  checking: 'hourglass_flowing_sand',
  added: 'white_check_mark',
  alreadyThere: 'blue_book',
  withTeam: 'eyes',
  needsInput: 'question',
  notAdded: 'no_entry_sign',
  problem: 'warning',
}

export const NEXT_STEP_RESEND =
  'Reply here mentioning me with the project name and the GitHub links, or ask the CDP team in this channel.'
export const NEXT_STEP_DISPUTE = 'Reply here if this looks wrong.'

export type ReviewCase =
  | 'new_project'
  | 'new_project_by_hand'
  | 'add_to_connection'
  | 'connect_github'
  | 'legacy_connection'
  | 'project_not_set_up'
  | 'not_in_pcc'
  | 'owner_only_match'
  | 'handed_off'
  | 'unknown'

export interface IReviewCaseCopy {
  insightsState: string
  whatsNeeded: string
  action: ReviewCardAction
}

interface IReviewCaseTemplate {
  insightsState: string
  whatsNeeded: (project: string, repos: string, many: boolean) => string
  action: ReviewCardAction
}

const FALLBACK_PROJECT_LABEL = 'the project'

function confirmProjectByHand(_: string, repos: string, many: boolean): string {
  return `Confirm which project ${repos} ${many ? 'belong' : 'belongs'} to, add ${many ? 'them' : 'it'} by hand, then mark as done`
}

const REVIEW_CASE_COPY: Record<ReviewCase, IReviewCaseTemplate> = {
  new_project: {
    insightsState: 'Not in LFX Insights yet',
    whatsNeeded: (_, repos) => `Create a new non-LF project in LFX Insights and add ${repos}`,
    action: 'create_project',
  },
  new_project_by_hand: {
    insightsState: 'Not in LFX Insights yet',
    whatsNeeded: (project) =>
      `Create ${project} as a non-LF project in CDP with these repositories by hand, then mark as done`,
    action: 'manual',
  },
  add_to_connection: {
    insightsState: 'Project exists with GitHub connected',
    whatsNeeded: (project, repos) => `Add ${repos} to ${project}'s GitHub connection`,
    action: 'add_to_connection',
  },
  connect_github: {
    insightsState: 'Project exists, GitHub not connected',
    whatsNeeded: (project, repos) => `Connect GitHub to ${project} with ${repos}`,
    action: 'connect_github',
  },
  legacy_connection: {
    insightsState: 'Uses the old GitHub connection',
    whatsNeeded: (_, repos, many) =>
      `Add ${many ? repos : 'the repo'} by hand in CDP, then mark as done`,
    action: 'manual',
  },
  project_not_set_up: {
    insightsState: 'Not set up yet',
    whatsNeeded: (project, repos) => `Create ${project} in CDP, add ${repos}, then mark as done`,
    action: 'manual',
  },
  not_in_pcc: {
    insightsState: 'Not in the foundation project list',
    whatsNeeded: (project, repos) =>
      `Check ${project} with PCC, then create it in CDP, add ${repos} and mark as done`,
    action: 'manual',
  },
  owner_only_match: {
    insightsState: 'Only the GitHub owner matches LF projects',
    whatsNeeded: confirmProjectByHand,
    action: 'manual',
  },
  handed_off: {
    insightsState: 'Handed to the CDP team by the requester',
    whatsNeeded: confirmProjectByHand,
    action: 'manual',
  },
  unknown: {
    insightsState: 'Not checked',
    whatsNeeded: confirmProjectByHand,
    action: 'manual',
  },
}

export const MAX_BUTTON_TEXT_LENGTH = 75

type TechnicalRow = Pick<IDbProjectCatalog, 'classificationKind' | 'proposedAction'>

export function repoLabel(repoUrl: string): string {
  const canonical = canonicalizeRepoUrl(repoUrl)
  if (!canonical?.isGithub) {
    return `*${escapeSlackText(repoUrl)}*`
  }
  return `*<${canonical.url}|${escapeSlackText(`${canonical.owner}/${canonical.repo}`)}>*`
}

export function describeReviewCase(
  reviewCase: ReviewCase,
  matchedName: string | null,
  repositories = 1,
): IReviewCaseCopy {
  const copy = REVIEW_CASE_COPY[reviewCase]
  const many = repositories > 1
  const repos = many ? `these ${repositories} repos` : 'this repo'
  return {
    insightsState: copy.insightsState,
    whatsNeeded: copy.whatsNeeded(matchedName ?? FALLBACK_PROJECT_LABEL, repos, many),
    action: copy.action,
  }
}

function trimTrailingSlash(url: string): string {
  return url.endsWith('/') ? url.slice(0, -1) : url
}

export function buildCdpSegmentUrl(
  frontendUrl: string,
  grandparentId: string,
  segmentId: string,
): string {
  return `${trimTrailingSlash(frontendUrl)}/integrations/${grandparentId}/${segmentId}`
}

export function buildCdpAdminUrl(frontendUrl: string): string {
  return `${trimTrailingSlash(frontendUrl)}/admin#project-groups`
}

export function technicalDetails({
  classificationKind,
  proposedAction,
}: TechnicalRow): string | null {
  const parts = [classificationKind, proposedAction].filter(
    (part): part is NonNullable<typeof part> => part !== null,
  )
  return parts.length > 0 ? `Details: ${parts.join(' · ')}` : null
}
