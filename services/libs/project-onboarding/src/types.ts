export interface IOnboardingInput {
  id: string
  repoUrl: string
  repoName: string
  projectSlug: string
}

export type OnboardingOutcome = 'onboarded' | 'error'

export interface IOnboardingResult {
  outcome: OnboardingOutcome
  segmentId: string | null
  error: string | null
}
