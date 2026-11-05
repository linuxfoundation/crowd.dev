export interface IOnboardingInput {
  id: string
  repoUrl: string
  repoName: string
  projectSlug: string
}

export interface IOrganizationProjectOnboardingInput {
  id: string
  projectName: string | null
  repoUrls: string[]
}

export interface ISegmentOnboardingInput {
  segmentId: string
  repoUrl: string
}

export interface ISegmentReposOnboardingInput {
  segmentId: string
  repoUrls: string[]
}

export interface IGithubRepoSettings {
  name: string
  url: string
  forkedFrom: string | null
  updatedAt: string
}

export interface IGithubOrgSettings {
  name: string
  url: string
  logo: string
  fullSync: boolean
  updatedAt: string
  repos: IGithubRepoSettings[]
}

export interface IGithubIntegrationSnapshot {
  id: string
  orgs: IGithubOrgSettings[]
}

export type OnboardingOutcome = 'onboarded' | 'error'

export interface IOnboardingResult {
  outcome: OnboardingOutcome
  segmentId: string | null
  error: string | null
}
