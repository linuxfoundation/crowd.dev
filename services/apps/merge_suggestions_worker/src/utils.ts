import { ILLMConsumableMember, PlatformType } from '@crowd/types'

import { EMAIL_AS_USERNAME_PLATFORMS } from './enums'

const NOREPLY_SUFFIXES = ['@users.noreply.github.com', '@users.noreply.gitlab.com']

function isNoreplyEmail(value: string): boolean {
  const lower = value.toLowerCase()
  return NOREPLY_SUFFIXES.some((s) => lower.endsWith(s))
}

export const prefixLength = (string: string) => {
  if (string.length > 5 && string.length < 8) {
    return 6
  }

  return 10
}

export function chunkArray<T>(array: T[], chunkSize: number): T[][] {
  const chunks = []
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize))
  }
  return chunks
}

export const removeEmailLikeIdentitiesFromMember = (
  member: ILLMConsumableMember,
): ILLMConsumableMember => {
  // Strip plain emails to avoid the LLM making false matches via shared domain suffixes.
  // Noreply addresses are kept — their local-part is a provider-authoritative username and
  // gives the LLM a meaningful identity signal.
  member.identities = member.identities.filter(
    (identity) => !identity.value.includes('@') || isNoreplyEmail(identity.value),
  )

  return member
}

export function isNumeric(value: string) {
  return !Number.isNaN(Number(value))
}

export function isEmailAsUsernamePlatform(platform: PlatformType) {
  return EMAIL_AS_USERNAME_PLATFORMS.includes(platform)
}

export function stripProtocol(value: string) {
  return value.replace(/^https?:\/\//, '')
}

// Generic git user.name placeholders and OS account names that are never a real human
// identity — shared across unrelated machines and meaningless as a merge signal.
const OS_RESERVED_NAMES = new Set(['unknown', 'root', 'ubuntu', 'admin', 'user', 'guest'])

export function isOsReservedName(name: string): boolean {
  return OS_RESERVED_NAMES.has(name.trim().toLowerCase())
}
