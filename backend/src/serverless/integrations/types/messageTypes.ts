import { MemberIdentityType, PlatformType } from '@crowd/types'

import { State } from './regularTypes'

export type IntegrationsMessage = {
  integration: string
  state: State
  tenant: string
  sleep: number
  onboarding: boolean
  args: object
}

export interface DevtoIntegrationMessage extends IntegrationsMessage {
  integrationId: string
}

export interface TwitterIntegrationMessage extends IntegrationsMessage {
  args: {
    profileId: string
    hashtags: Array<string>
  }
}

export interface TwitterReachMessage extends IntegrationsMessage {
  args: {
    profileId: string
  }
}

export interface SlackIntegrationMessage extends IntegrationsMessage {
  args: {
    channels?: any
  }
}

export interface GithubIntegrationMessage extends IntegrationsMessage {}

export interface MemberIdentity {
  value: string
  type: MemberIdentityType
  integrationId: string
  sourceId?: string
}

export type PlatformIdentities = {
  [K in keyof typeof PlatformType]?: [MemberIdentity]
}

export type Member = {
  username: PlatformIdentities
  displayName?: string
  attributes?: any
  emails?: string[]
  organizations?: any[]
  bio?: string
  reach?: number | any
  location?: string
  enrichedBy?: string[] | null
  contributions?: any
}

export type AddActivitiesSingle = {
  timestamp: Date
  type: string
  username: string
  platform: string
  tenant: string
  member: Member
  objectMember?: Member
  objectMemberUsername?: string
  sourceId?: string
  sourceParentId?: string
  attributes?: object
  body?: string
  title?: string
  url?: string
  channel?: string
  score?: number
}

export type AddActivities = Array<AddActivitiesSingle>

export type Update = { id: string; update: Object }
export type Updates = Array<Update>

export type DbOperations = {
  operation: string
  records: AddActivities | Updates
  tenantId: string
}

export type GraphQlQueryResponse = {
  hasPreviousPage: boolean
  startCursor: string
  data: any[]
}
