import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'
import {
  ISlackMessageContent,
  describeDecisionReason,
  escapeSlackText,
  linkButtonBlocks,
  mrkdwnSections,
} from '@crowd/slack'

import {
  INSIGHTS_PROJECT_URL_BASE,
  NEXT_STEP_DISPUTE,
  NEXT_STEP_RESEND,
  RETRY_EVALUATION_ACTION_ID,
  STATUS,
  VIEW_INSIGHTS_PROJECT_ACTION_ID,
  repoLabel,
} from './botCopy'
import { IOnboardedInsightsProject } from './onboardedProjectLookup'
import { deriveProjectSlug } from './onboarder'
import { IOrganizationReview, hasManyRepositories, organizationSubject } from './organizationGroup'
import { resolveReviewCase } from './reviewCard'

const MAX_REASON_LENGTH = 500
const DATA_SYNC_NOTE = 'Activity shows up after the first data sync.'

type BotReplyProject = Pick<IDbProjectCatalog, 'repoUrl'>
type BotSkippedProject = Pick<IDbProjectCatalog, 'repoUrl' | 'classificationKind'>
type BotReviewProject = Pick<
  IDbProjectCatalog,
  'repoUrl' | 'reviewReason' | 'proposedAction' | 'classificationKind' | 'matchedSegmentId'
>
type BotOnboardedProject = Pick<IDbProjectCatalog, 'repoUrl' | 'projectSlug' | 'proposedAction'>
type BotClosedProject = BotOnboardedProject & Pick<IDbProjectCatalog, 'reviewedBy'>
type BotReviewedProject = Pick<IDbProjectCatalog, 'repoUrl' | 'reviewedBy'>
type BotRejectedProject = BotReviewedProject &
  Pick<IDbProjectCatalog, 'decisionReason' | 'decisionNote'>

export function buildInsightsProjectUrl(projectSlug: string): string {
  return `${INSIGHTS_PROJECT_URL_BASE}/${deriveProjectSlug(projectSlug)}`
}

function repo(project: BotReplyProject, organization: IOrganizationReview | null = null): string {
  return organization ? organizationSubject(organization) : repoLabel(project.repoUrl)
}

function wordFor(organization: IOrganizationReview | null, many: string, one: string): string {
  return hasManyRepositories(organization) ? many : one
}

function stripTrailingPeriods(text: string): string {
  let end = text.length
  while (end > 0 && (text[end - 1] === '.' || text[end - 1] === ' ')) {
    end -= 1
  }
  return text.slice(0, end)
}

export function formatReason(reason: string): string {
  const trimmed = stripTrailingPeriods(reason.trim())
  const truncated =
    trimmed.length > MAX_REASON_LENGTH ? `${trimmed.slice(0, MAX_REASON_LENGTH)}…` : trimmed
  return escapeSlackText(truncated)
}

function reply(...lines: string[]): string {
  return lines.join('\n')
}

export function buildBotSkippedReply(
  project: BotSkippedProject,
  reason: string,
  organization: IOrganizationReview | null = null,
): string {
  if (project.classificationKind === 'already_tracked') {
    return `${STATUS.alreadyThere} ${repo(project)} is already in LFX Insights. Nothing else to do.`
  }
  return reply(
    `${STATUS.notAdded} ${repo(project, organization)} ${wordFor(organization, "weren't", "wasn't")} added: ${formatReason(reason)}.`,
    NEXT_STEP_RESEND,
  )
}

function reviewNextStep(project: BotReviewProject): string {
  switch (resolveReviewCase(project)) {
    case 'legacy_connection':
      return "I've asked the CDP team to add it by hand; they'll mark it done here once it's in."
    case 'project_not_set_up':
      return "I've asked the CDP team to create it and add the repo; they'll let you know here."
    case 'not_in_pcc':
      return "I've asked the CDP team to check it with PCC and add it; they'll let you know here."
    case 'new_project_by_hand':
      return "I've asked the CDP team to create it and add the repositories; they'll let you know here."
    case 'owner_only_match':
    case 'handed_off':
    case 'unknown':
      return "I've asked the CDP team to pick the right project and add it by hand; they'll confirm here."
    case 'new_project':
      return "I've sent your request to the CDP team; they'll approve or reject it here."
    default:
      return "The CDP team will approve or reject it here, and I'll post the result."
  }
}

export function buildBotReviewReply(
  project: BotReviewProject,
  organization: IOrganizationReview | null = null,
): string {
  const subject = repo(project, organization)
  const summary = project.reviewReason
    ? `${STATUS.withTeam} ${subject}: ${formatReason(project.reviewReason)}.`
    : `${STATUS.withTeam} ${subject} ${wordFor(organization, 'need', 'needs')} a quick check before I add ${wordFor(organization, 'them', 'it')}.`
  return `${summary} ${reviewNextStep(project)}`
}

export function buildBotOnboardedReply(project: BotOnboardedProject): string {
  if (
    project.proposedAction === 'create_integration' ||
    project.proposedAction === 'update_integration'
  ) {
    return `${STATUS.added} ${repo(project)} is now in LFX Insights, in its existing project. ${DATA_SYNC_NOTE}`
  }

  return `${STATUS.added} ${repo(project)} is now in LFX Insights: <${buildInsightsProjectUrl(project.projectSlug)}|View project>. ${DATA_SYNC_NOTE}`
}

export function buildBotOnboardedContent(
  project: BotClosedProject,
  insightsProject: IOnboardedInsightsProject | null,
): ISlackMessageContent {
  if (!insightsProject) {
    return { text: buildBotOnboardedReply(project) }
  }

  const approvedBy = project.reviewedBy ? ` Approved by <@${project.reviewedBy}>.` : ''
  const text = `${STATUS.added} ${repo(project)} is now in LFX Insights under *${escapeSlackText(insightsProject.name)}*.${approvedBy}`
  return {
    text: `${text} ${DATA_SYNC_NOTE}`,
    blocks: [
      ...mrkdwnSections(text),
      { type: 'context', elements: [{ type: 'mrkdwn', text: DATA_SYNC_NOTE }] },
      ...linkButtonBlocks([
        {
          text: `View ${insightsProject.name} in LFX Insights`,
          url: `${INSIGHTS_PROJECT_URL_BASE}/${insightsProject.slug}`,
          actionId: `${VIEW_INSIGHTS_PROJECT_ACTION_ID}:0`,
        },
      ]),
    ],
  }
}

export function buildBotEvaluationErrorContent(
  project: Pick<IDbProjectCatalog, 'id' | 'repoUrl'>,
): ISlackMessageContent {
  const text = `${STATUS.problem} Something went wrong on our side while checking ${repo(project)}. Your request is saved and the CDP team has been alerted.`
  return {
    text,
    blocks: [
      ...mrkdwnSections(text),
      {
        type: 'actions',
        elements: [
          {
            type: 'button',
            action_id: RETRY_EVALUATION_ACTION_ID,
            value: project.id,
            text: { type: 'plain_text', text: 'Try again' },
          },
        ],
      },
    ],
  }
}

export function buildBotErroredReply(project: BotReplyProject): string {
  return reply(
    `${STATUS.problem} Something went wrong on our side while adding ${repo(project)}. Your request is saved and the CDP team can see the error.`,
    "Ask the CDP team in this channel if you don't hear back.",
  )
}

function reviewer(project: BotReviewedProject, fallback: string): string {
  return project.reviewedBy ? `<@${project.reviewedBy}>` : fallback
}

function quotedNote(note: string | null): string[] {
  const trimmed = note?.trim()
  return trimmed ? [`> ${escapeSlackText(trimmed).split('\n').join('\n> ')}`] : []
}

export function buildBotRejectedReply(
  project: BotRejectedProject,
  organization: IOrganizationReview | null = null,
): string {
  const reason = describeDecisionReason(project.decisionReason)
  const verdict = reason
    ? `${reviewer(project, 'The CDP team')} reviewed it: *${reason}*.`
    : `${reviewer(project, 'The CDP team')} reviewed it and rejected the request.`
  return reply(
    `${STATUS.notAdded} ${repo(project, organization)} ${wordFor(organization, "weren't", "wasn't")} added. ${verdict}`,
    ...quotedNote(project.decisionNote),
    NEXT_STEP_DISPUTE,
  )
}

export function buildBotDoneReply(
  project: BotReviewedProject,
  organization: IOrganizationReview | null = null,
): string {
  return `${STATUS.added} ${repo(project, organization)} ${wordFor(organization, 'were', 'was')} added to LFX Insights by hand by ${reviewer(project, 'the CDP team')}. ${DATA_SYNC_NOTE}`
}
