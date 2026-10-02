import { IPccCandidate, OnboardingResolution } from '@crowd/project-onboarding'
import { SlackMessageSection } from '@crowd/slack'

import { IRequestClassificationAlert } from './requestClassification'

const ALERT_TITLES: Record<OnboardingResolution['kind'], string> = {
  non_github_source: 'Onboarding request without GitHub repositories',
  non_lf_new_project: 'Onboarding request for a new project',
  lf_not_in_pcc: 'LF onboarding request: project not found in PCC',
  lf_not_in_cdp: 'LF onboarding request: project not in CDP yet',
  lf_in_cdp: 'LF onboarding request: project already in CDP',
  ambiguous: 'Onboarding request needs review',
}

const INTEGRATION_ACTION_LABELS = {
  create_integration: 'Create the GitHub integration',
  update_integration: 'Update the existing GitHub connection (do not disconnect)',
  human_review: 'Human review: GitHub v1 integration, migration to v2 needed',
} as const

function formatCandidate(candidate: IPccCandidate): string {
  const level = candidate.isLeaf ? 'project' : 'parent group'
  return `• ${candidate.name} (${candidate.slug}), score ${candidate.score.toFixed(2)}, ${level}`
}

function formatCandidates(candidates: IPccCandidate[]): string {
  return candidates.map(formatCandidate).join('\n')
}

function resolutionSections(resolution: OnboardingResolution): SlackMessageSection[] {
  switch (resolution.kind) {
    case 'non_github_source':
      return [{ title: 'Repositories', text: resolution.nonGithubRepoUrls.join('\n') }]
    case 'non_lf_new_project':
      return []
    case 'lf_not_in_pcc':
      return [{ title: 'Project name', text: resolution.projectName }]
    case 'lf_not_in_cdp':
      return [{ title: 'PCC project', text: formatCandidate(resolution.pccProject) }]
    case 'lf_in_cdp':
      return [
        { title: 'PCC project', text: formatCandidate(resolution.pccProject) },
        {
          title: 'CDP segment',
          text: `${resolution.segment.name}\nIntegration: ${resolution.segment.integration}`,
        },
        { title: 'Proposed action', text: INTEGRATION_ACTION_LABELS[resolution.action] },
      ]
    case 'ambiguous':
      return [
        { title: 'Reason', text: resolution.reason },
        ...(resolution.candidates.length > 0
          ? [{ title: 'PCC candidates', text: formatCandidates(resolution.candidates) }]
          : []),
      ]
  }
}

export function buildRequestClassificationAlertTitle(alert: IRequestClassificationAlert): string {
  return ALERT_TITLES[alert.resolution.kind]
}

export function buildRequestClassificationAlert(
  alert: IRequestClassificationAlert,
): SlackMessageSection[] {
  const requestedIn = alert.sourceUrl
    ? `<${alert.sourceUrl}|${alert.sourceUrl}>`
    : '_source discussion not recorded_'

  return [
    {
      title: '',
      text: [`Requested in: ${requestedIn}`, ...alert.repoUrls].join('\n'),
    },
    ...resolutionSections(alert.resolution),
  ]
}
