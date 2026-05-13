import { IMemberIdentity } from '@crowd/types'

export interface MemberUpdateInput {
  member: {
    id: string
  }
  memberOrganizationIds?: string[]
  syncToOpensearch?: boolean
}

export interface ProcessMemberBotSuggestionWithLLMInput {
  memberId: string
}

export interface MemberForLLMBotSuggestion {
  displayName: string
  identities: IMemberIdentity[]
  bio?: Record<string, string>
}

export enum MemberBotSignalType {
  IDENTITIES = 'identities',
  BIO = 'bio',
  DISPLAY_NAME = 'displayName',
}

export enum MemberBotSignalStrength {
  STRONG = 'strong',
  MEDIUM = 'medium',
  WEAK = 'weak',
}

export type MemberBotSignal = Partial<Record<MemberBotSignalType, MemberBotSignalStrength>>

export interface MemberBotSuggestionResult {
  isBot: boolean
  signals: MemberBotSignal
  // reason for the decision but only used for debugging purposes
  reason: string
}
