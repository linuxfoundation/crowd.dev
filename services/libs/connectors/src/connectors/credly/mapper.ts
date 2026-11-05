import { z } from 'zod'

import { getEmailLocalPart } from '@crowd/common'
import type { ICredlyBadgeRow } from '@crowd/data-access-layer/src/credly/badges'
import { CREDLY_GRID, CredlyActivityType } from '@crowd/integrations/src/integrations/credly/types'
import { MemberIdentityType, PlatformType } from '@crowd/types'

import type { CredlyBadge, CredlyBadgeState } from './badge'

const CREDLY_BADGE_URL_PREFIX = 'https://www.credly.com/badges/'

const STATE_ACTIVITY_TYPES: Record<Exclude<CredlyBadgeState, 'pending'>, CredlyActivityType> = {
  accepted: CredlyActivityType.ACCEPTED_BADGE,
  rejected: CredlyActivityType.REJECTED_BADGE,
  revoked: CredlyActivityType.REVOKED_BADGE,
  replaced: CredlyActivityType.REPLACED_BADGE,
}

const identitySchema = z.object({
  platform: z.literal(PlatformType.CREDLY),
  value: z.string().min(1),
  type: z.nativeEnum(MemberIdentityType),
  verified: z.literal(true),
  sourceId: z.string().min(1).optional(),
})

const memberSchema = z.object({
  displayName: z.string().min(1),
  identities: z.array(identitySchema).min(1),
  attributes: z.record(z.unknown()).optional(),
})

export const credlyActivitySchema = z.object({
  type: z.nativeEnum(CredlyActivityType),
  platform: z.literal(PlatformType.CREDLY),
  timestamp: z.string(),
  sourceId: z.string().min(1),
  score: z.number(),
  title: z.string(),
  url: z.string(),
  channel: z.string().min(1),
  attributes: z.record(z.unknown()),
  member: memberSchema,
})

export type CredlyActivity = z.infer<typeof credlyActivitySchema>

// A renewed badge is born with the dates of the badge it replaces copied onto it
// (linuxfoundation/lfx-cdp-insights-ops#967): only created_at dates the renewal.
export function issuedAt(badge: CredlyBadge): string {
  return badge.replaces_badge_id && badge.created_at ? badge.created_at : badge.issued_at
}

export function stateChangedAt(badge: CredlyBadge): string {
  const stateUpdatedAt = badge.state_updated_at ?? badge.issued_at
  if (!badge.created_at) return stateUpdatedAt
  return Date.parse(badge.created_at) > Date.parse(stateUpdatedAt)
    ? badge.created_at
    : stateUpdatedAt
}

export function issuedBadgeActivity(badge: CredlyBadge): CredlyActivity {
  return badgeActivity(badge, CredlyActivityType.ISSUED_BADGE, issuedAt(badge))
}

export function stateBadgeActivity(badge: CredlyBadge): CredlyActivity | null {
  if (badge.state === 'pending') return null
  return badgeActivity(badge, STATE_ACTIVITY_TYPES[badge.state], stateChangedAt(badge))
}

export function expiredBadgeActivity(badge: CredlyBadge): CredlyActivity | null {
  if (badge.state !== 'accepted' || !badge.expires_at) return null
  return badgeActivity(badge, CredlyActivityType.EXPIRED_BADGE, badge.expires_at)
}

export function expiredBadgeActivityFromRow(row: ICredlyBadgeRow): CredlyActivity {
  if (!row.expiresAt) throw new Error(`credly badge ${row.badgeId} has no expiry date`)
  const type = CredlyActivityType.EXPIRED_BADGE
  return {
    type,
    platform: PlatformType.CREDLY,
    timestamp: toIsoTimestamp(row.expiresAt),
    sourceId: row.badgeId,
    score: CREDLY_GRID[type].score,
    title: row.badgeTemplateName,
    url: badgeUrl({ id: row.badgeId, badge_url: null }),
    channel: row.badgeTemplateId,
    attributes: withoutNullish({
      badgeTemplateId: row.badgeTemplateId,
      badgeTemplateName: row.badgeTemplateName,
      badgeTemplateUrl: row.badgeTemplateUrl,
      state: row.state,
      issuedAt: toIsoTimestamp(row.issuedAt),
      expiresAt: toIsoTimestamp(row.expiresAt),
      replacementBadgeId: row.replacementBadgeId,
      replacesBadgeId: row.replacesBadgeId,
    }),
    member: memberFromEarner(row.recipientEmail, row.issuedTo, undefined),
  }
}

export function badgeUrl(badge: Pick<CredlyBadge, 'id' | 'badge_url'>): string {
  return badge.badge_url || `${CREDLY_BADGE_URL_PREFIX}${badge.id}`
}

export function toIsoTimestamp(raw: string): string {
  const parsed = new Date(raw)
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`credly timestamp is not a valid date/time: ${raw}`)
  }
  return parsed.toISOString()
}

function badgeActivity(
  badge: CredlyBadge,
  type: CredlyActivityType,
  timestamp: string,
): CredlyActivity {
  const template = badge.badge_template
  return {
    type,
    platform: PlatformType.CREDLY,
    timestamp: toIsoTimestamp(timestamp),
    sourceId: badge.id,
    score: CREDLY_GRID[type].score,
    title: template.name,
    url: badgeUrl(badge),
    channel: template.id,
    attributes: withoutNullish({
      badgeTemplateId: template.id,
      badgeTemplateName: template.name,
      badgeTemplateUrl: template.url,
      badgeTemplateSlug: template.vanity_slug,
      badgeTemplateState: template.state,
      typeCategory: template.type_category,
      level: template.level,
      skills: template.skills,
      globalActivityUrl: template.global_activity_url,
      state: badge.state,
      issuedAt: badge.issued_at,
      createdAt: badge.created_at,
      expiresAt: badge.expires_at,
      stateUpdatedAt: badge.state_updated_at,
      revocationReason: badge.revocation_reason,
      replacementBadgeId: badge.replacement_badge_id,
      replacesBadgeId: badge.replaces_badge_id,
      issuerEarnerId: badge.issuer_earner_id,
      imageUrl: badge.image_url,
      public: badge.public,
    }),
    member: badgeMember(badge),
  }
}

function badgeMember(badge: CredlyBadge): CredlyActivity['member'] {
  const attributes = withoutNullish({
    url: badge.user?.url ? { [PlatformType.CREDLY]: badge.user.url } : undefined,
    avatarUrl: badge.user?.photo_url ? { [PlatformType.CREDLY]: badge.user.photo_url } : undefined,
  })
  return {
    ...memberFromEarner(
      badge.recipient_email,
      badge.issued_to,
      badge.user?.id?.trim() || undefined,
    ),
    ...(Object.keys(attributes).length > 0 && { attributes }),
  }
}

function memberFromEarner(
  recipientEmail: string,
  issuedTo: string | null | undefined,
  userId: string | undefined,
): CredlyActivity['member'] {
  const email = recipientEmail.trim().toLowerCase()
  return {
    displayName: issuedTo?.trim() || getEmailLocalPart(email),
    identities: emailAsUsernameAndEmailIdentities(email, userId),
  }
}

function emailAsUsernameAndEmailIdentities(
  email: string,
  sourceId: string | undefined,
): CredlyActivity['member']['identities'] {
  return [MemberIdentityType.USERNAME, MemberIdentityType.EMAIL].map((type) => ({
    platform: PlatformType.CREDLY,
    value: email,
    type,
    verified: true,
    sourceId,
  }))
}

function withoutNullish(obj: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(obj).filter(([, v]) => v !== null && v !== undefined))
}
