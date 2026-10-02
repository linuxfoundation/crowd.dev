import { IParsedOnboardingRequest } from './requestParser'

const STRONG_MATCH_SCORE = 0.97
const WEAK_MATCH_SCORE = 0.85
const GITHUB_V1_PLATFORM = 'github'
const GITHUB_NANGO_PLATFORM = 'github-nango'

export interface IPccCandidate {
  projectId: string
  name: string
  slug: string
  score: number
}

export type CdpIntegrationState = 'none' | 'github-nango' | 'github-v1'

export interface ICdpSegmentMatch {
  segmentId: string
  name: string
  integration: CdpIntegrationState
}

export interface IOnboardingRequestLookups {
  findPccCandidates?: (projectName: string) => Promise<IPccCandidate[]>
  findCdpSegmentByPccProject: (pccProjectId: string) => Promise<ICdpSegmentMatch | null>
}

export type CdpIntegrationAction = 'create_integration' | 'update_integration' | 'human_review'

export type OnboardingResolution =
  | { kind: 'non_lf_new_project'; projectName: string }
  | { kind: 'lf_not_in_pcc'; projectName: string }
  | { kind: 'lf_not_in_cdp'; pccProject: IPccCandidate }
  | {
      kind: 'lf_in_cdp'
      pccProject: IPccCandidate
      segment: ICdpSegmentMatch
      action: CdpIntegrationAction
    }
  | { kind: 'ambiguous'; reason: string; candidates: IPccCandidate[] }

const INTEGRATION_ACTIONS: Record<CdpIntegrationState, CdpIntegrationAction> = {
  none: 'create_integration',
  'github-nango': 'update_integration',
  'github-v1': 'human_review',
}

export function toCdpIntegrationState(platforms: string[]): CdpIntegrationState {
  if (platforms.includes(GITHUB_V1_PLATFORM)) {
    return 'github-v1'
  }

  return platforms.includes(GITHUB_NANGO_PLATFORM) ? 'github-nango' : 'none'
}

function normalizeName(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]/g, '')
}

function isExactMatch(projectName: string, candidate: IPccCandidate): boolean {
  const normalized = normalizeName(projectName)
  return (
    normalized === normalizeName(candidate.name) || normalized === normalizeName(candidate.slug)
  )
}

function isStrongMatch(projectName: string, candidate: IPccCandidate): boolean {
  return isExactMatch(projectName, candidate) || candidate.score >= STRONG_MATCH_SCORE
}

function rankCandidates(candidates: IPccCandidate[]): IPccCandidate[] {
  return [...candidates].sort((a, b) => b.score - a.score)
}

function hasNoRepositories(request: IParsedOnboardingRequest): boolean {
  return request.githubRepoUrls.length === 0 && request.nonGithubRepoUrls.length === 0
}

function ambiguous(reason: string, candidates: IPccCandidate[] = []): OnboardingResolution {
  return { kind: 'ambiguous', reason, candidates }
}

async function resolveStrongMatch(
  request: IParsedOnboardingRequest,
  pccProject: IPccCandidate,
  lookups: IOnboardingRequestLookups,
): Promise<OnboardingResolution> {
  if (request.declaredLf === false) {
    return ambiguous('Request says the project is not LF but it matches a PCC project', [
      pccProject,
    ])
  }

  const segment = await lookups.findCdpSegmentByPccProject(pccProject.projectId)
  if (!segment) {
    return { kind: 'lf_not_in_cdp', pccProject }
  }

  return {
    kind: 'lf_in_cdp',
    pccProject,
    segment,
    action: INTEGRATION_ACTIONS[segment.integration],
  }
}

export async function resolveOnboardingRequest(
  request: IParsedOnboardingRequest,
  lookups: IOnboardingRequestLookups,
): Promise<OnboardingResolution> {
  if (request.asksAboutHierarchy) {
    return ambiguous('Requester asks about the project hierarchy')
  }

  if (hasNoRepositories(request) && request.linksToFollow.length === 0) {
    return ambiguous('Request does not contain any repository')
  }

  const { projectName } = request
  if (!projectName) {
    return ambiguous('Project name could not be determined')
  }

  if (!lookups.findPccCandidates) {
    return ambiguous('PCC lookup is not configured')
  }

  const candidates = rankCandidates(await lookups.findPccCandidates(projectName))
  const [best] = candidates

  if (best && isStrongMatch(projectName, best)) {
    return resolveStrongMatch(request, best, lookups)
  }

  const weakCandidates = candidates.filter((candidate) => candidate.score >= WEAK_MATCH_SCORE)
  if (weakCandidates.length > 0) {
    return ambiguous('Project name only loosely matches PCC projects', weakCandidates)
  }

  if (request.declaredLf) {
    return { kind: 'lf_not_in_pcc', projectName }
  }

  return { kind: 'non_lf_new_project', projectName }
}
