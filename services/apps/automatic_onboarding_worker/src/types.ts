import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'

export interface IOnboardProjectsInput {
  batchSize?: number
  catalogIds?: string[]
}

export interface IDeletedProjectSkips {
  skipped: IDbProjectCatalog[]
  remaining: IDbProjectCatalog[]
}

export interface IOrganizationOnboardingOutcome {
  onboarded: IDbProjectCatalog[]
  alreadyHandled: number
  racedOut: number
}

export type OnboardAndUpdateProjectOutcome =
  | 'onboarded'
  | 'skipped'
  | 'already-skipped'
  | 'already-onboarded'
  | 'catalog-changed'
