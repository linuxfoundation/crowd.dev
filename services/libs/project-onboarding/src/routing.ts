import {
  IDbProjectCatalog,
  IDbProjectCatalogClassification,
  IRequestQuestion,
  ProjectCatalogAction,
  ProjectCatalogRoutedAction,
  ProjectEvaluationOutcome,
  isSlackBotProvenance,
} from '@crowd/data-access-layer/src/project-catalog/types'
import { IReviewOrganization, organizationLabel, repositoriesWord } from '@crowd/slack'

import { deriveOrganizationProjectNameCandidates } from './onboarder'
import { OnboardingResolution } from './requestResolver'

export interface IRoutingOptions {
  autoApply: boolean
  canAsk?: boolean
}

export type RoutedResolution = Pick<
  IDbProjectCatalogClassification,
  'action' | 'skipReason' | 'reviewReason' | 'requestQuestion'
>

export const ALREADY_TRACKED_SKIP_REASON = 'it is already in LFX Insights'
export const NON_GITHUB_SOURCE_SKIP_REASON = 'only GitHub repositories can be added'

function route(action: ProjectCatalogRoutedAction): RoutedResolution {
  return { action, skipReason: null, reviewReason: null, requestQuestion: null }
}

function skip(skipReason: string): RoutedResolution {
  return { action: 'skip', skipReason, reviewReason: null, requestQuestion: null }
}

function review(reviewReason: string): RoutedResolution {
  return { action: 'review', skipReason: null, reviewReason, requestQuestion: null }
}

function ask(question: IRequestQuestion): RoutedResolution {
  return { action: 'ask', skipReason: null, reviewReason: null, requestQuestion: question }
}

function lfNotInPccReviewReason(projectName: string): string {
  return `${projectName} is described as an LF project, but it isn't in the foundation project list yet`
}

function describeOwners(owners: string[]): string {
  return owners.length === 1
    ? `the GitHub owner ${owners[0]} only has`
    : `the GitHub owners ${owners.join(', ')} only have`
}

function lfInCdpReviewReason({
  segment,
  action,
}: Extract<OnboardingResolution, { kind: 'lf_in_cdp' }>): string {
  return action === 'human_review'
    ? `${segment.name} still uses an older GitHub connection, so I can't add repositories to it myself`
    : `${segment.name} is in LFX Insights, but this repo needs a quick check by the CDP team before I add it`
}

export function routeResolution(
  resolution: OnboardingResolution,
  { autoApply, canAsk = false }: IRoutingOptions,
): RoutedResolution {
  switch (resolution.kind) {
    case 'needs_input':
      return canAsk ? ask(resolution.question) : review(resolution.fallbackReason)
    case 'non_lf_new_project':
      return route('evaluate')
    case 'lf_in_cdp':
      return resolution.action !== 'human_review' && autoApply
        ? route('onboard')
        : review(lfInCdpReviewReason(resolution))
    case 'lf_not_in_cdp':
      return review(
        `${resolution.pccProject.name} is an LF project, but it isn't set up in LFX Insights yet`,
      )
    case 'lf_owner_in_cdp':
      return review(
        `${describeOwners(resolution.owners)} LF projects in LFX Insights, so I couldn't tell which one this repo belongs to`,
      )
    case 'already_tracked':
      return skip(ALREADY_TRACKED_SKIP_REASON)
    case 'non_github_source':
      return skip(NON_GITHUB_SOURCE_SKIP_REASON)
    case 'lf_not_in_pcc':
      return review(lfNotInPccReviewReason(resolution.projectName))
    case 'ambiguous':
      return skip(resolution.reason)
  }
}

export interface IEvaluationRoute {
  action: ProjectCatalogAction
  reviewReason: string | null
}

type EvaluatedRow = Pick<IDbProjectCatalog, 'provenance' | 'projectName' | 'repoName'>

interface IEvaluationVerdict {
  outcome: ProjectEvaluationOutcome
  evaluationResult: string
  evaluationReason: string | null
}

export const EVALUATION_ERROR_RESULT = 'error'

const NEW_PROJECTS_BY_CDP_TEAM = 'and new projects are added by the CDP team'

export function newProjectReviewReason(
  row: Pick<EvaluatedRow, 'projectName' | 'repoName'>,
  organization: IReviewOrganization | null = null,
): string {
  if (organization) {
    const [name] = deriveOrganizationProjectNameCandidates(organization.owner, row.projectName)
    return `${name} isn't in LFX Insights yet; it would be created as a non-LF project with the ${organization.repositories} ${repositoriesWord(organization.repositories)} of ${organizationLabel(organization.owner)}, ${NEW_PROJECTS_BY_CDP_TEAM}`
  }
  return `${row.projectName ?? row.repoName} isn't in LFX Insights yet; it would be created as a non-LF project, ${NEW_PROJECTS_BY_CDP_TEAM}`
}

export function unsureReviewReason(evaluationReason: string | null): string {
  const detail = evaluationReason?.trim()
  return detail
    ? `the automatic check couldn't decide: ${detail}`
    : "the automatic check couldn't decide"
}

export interface IEvaluationRoutingOptions {
  autoApply: boolean
  organization?: IReviewOrganization | null
}

// With autoApply off a new project must not be onboarded without a person; the evaluator's
// own errors are kept as error so the requester can retry them.
export function routeEvaluationOutcome(
  row: EvaluatedRow,
  verdict: IEvaluationVerdict,
  { autoApply, organization = null }: IEvaluationRoutingOptions,
): IEvaluationRoute {
  if (verdict.outcome === 'onboard' && !autoApply && isSlackBotProvenance(row.provenance)) {
    return { action: 'review', reviewReason: newProjectReviewReason(row, organization) }
  }
  if (verdict.evaluationResult === EVALUATION_ERROR_RESULT) {
    return { action: 'error', reviewReason: null }
  }
  if (verdict.outcome === 'unsure') {
    return { action: 'review', reviewReason: unsureReviewReason(verdict.evaluationReason) }
  }
  return { action: verdict.outcome, reviewReason: null }
}
