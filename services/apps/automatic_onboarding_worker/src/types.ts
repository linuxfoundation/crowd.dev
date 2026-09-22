export interface IOnboardProjectsInput {
  batchSize?: number
}

export type OnboardAndUpdateProjectOutcome =
  | 'onboarded'
  | 'skipped'
  | 'already-onboarded'
  | 'catalog-changed'
