import {
  IntegrationResultType,
  IntegrationState,
  IntegrationStreamType,
} from './enums/integrations'
import { PlatformType } from './enums/platforms'
import { IntegrationSettingsMap } from './integrationSettings'

export interface IIntegrationStream {
  identifier: string
  type: IntegrationStreamType
  data?: unknown
  webhookCreatedAt?: string
}

export interface IIntegrationResult {
  type: IntegrationResultType
  segmentId?: string
  data: unknown
}

export interface IIntegration<P extends PlatformType = PlatformType> {
  id: string
  identifier: string
  platform: P
  status: IntegrationState
  settings: P extends keyof IntegrationSettingsMap ? IntegrationSettingsMap[P] : unknown
  token: string | null
  refreshToken: string | null
}
