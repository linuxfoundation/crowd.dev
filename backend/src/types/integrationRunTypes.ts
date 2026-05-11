import { IntegrationRunState } from '@crowd/types'

export interface IntegrationRun {
  id: string
  tenantId: string
  integrationId: string | null
  onboarding: boolean
  state: IntegrationRunState
  delayedUntil: string | null
  processedAt: string | null
  error: any | null
  createdAt: string
  updatedAt: string
}
