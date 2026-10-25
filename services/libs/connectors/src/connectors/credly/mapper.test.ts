import { describe, expect, it } from 'vitest'

import { CredlyActivityType } from '@crowd/integrations/src/integrations/credly/types'
import { MemberIdentityType, PlatformType } from '@crowd/types'

import { credlyBadgeSchema } from './badge'
import type { CredlyBadge } from './badge'
import {
  badgeUrl,
  credlyActivitySchema,
  expiredBadgeActivity,
  issuedBadgeActivity,
  stateBadgeActivity,
  toIsoTimestamp,
} from './mapper'

const BASE_BADGE: CredlyBadge = {
  id: '0319b29d-1111-4ccc-8ddd-eeeeffff0001',
  state: 'accepted',
  issued_at: '2026-10-19T00:00:00.000Z',
  expires_at: '2028-10-18T00:00:00.000Z',
  state_updated_at: '2026-10-21T09:18:19.923Z',
  recipient_email: 'Maria.Rossi@Example.org',
  issued_to: 'Maria Rossi',
  issuer_earner_id: 'LF-kjdz03vcf8',
  badge_url: 'https://www.credly.com/badges/0319b29d-1111-4ccc-8ddd-eeeeffff0001',
  image_url: 'https://images.credly.com/images/cka.png',
  public: true,
  revocation_reason: null,
  replacement_badge_id: null,
  replaces_badge_id: null,
  user: {
    id: '7f1c2d3e-aaaa-4bbb-8ccc-dddd00000002',
    url: 'https://www.credly.com/users/maria-rossi',
    photo_url: null,
    confirmed: true,
  },
  badge_template: {
    id: 'b2c3d4e5-2222-4333-8444-555566667777',
    name: 'CKA: Certified Kubernetes Administrator',
    url: 'https://www.credly.com/org/the-linux-foundation/badge/cka-certified-kubernetes-administrator',
    vanity_slug: 'cka-certified-kubernetes-administrator',
    state: 'active',
    type_category: 'Certification',
    level: 'Intermediate',
    skills: ['Kubernetes'],
    global_activity_url:
      'https://training.linuxfoundation.org/certification/certified-kubernetes-administrator-cka/',
  },
}

describe('issuedBadgeActivity', () => {
  it('maps a badge to an issued-badge activity with both verified identities', () => {
    const activity = issuedBadgeActivity(BASE_BADGE)

    expect(activity).toMatchObject({
      type: CredlyActivityType.ISSUED_BADGE,
      platform: PlatformType.CREDLY,
      timestamp: '2026-10-19T00:00:00.000Z',
      sourceId: BASE_BADGE.id,
      score: 1,
      title: 'CKA: Certified Kubernetes Administrator',
      url: BASE_BADGE.badge_url,
      channel: BASE_BADGE.badge_template.id,
      member: {
        displayName: 'Maria Rossi',
        identities: [
          {
            platform: PlatformType.CREDLY,
            value: 'maria.rossi@example.org',
            type: MemberIdentityType.USERNAME,
            verified: true,
            sourceId: '7f1c2d3e-aaaa-4bbb-8ccc-dddd00000002',
          },
          {
            platform: PlatformType.CREDLY,
            value: 'maria.rossi@example.org',
            type: MemberIdentityType.EMAIL,
            verified: true,
            sourceId: '7f1c2d3e-aaaa-4bbb-8ccc-dddd00000002',
          },
        ],
        attributes: {
          url: { credly: 'https://www.credly.com/users/maria-rossi' },
        },
      },
    })
    expect(activity.member.attributes).not.toHaveProperty('avatarUrl')
  })

  it('keeps the badge and template context in attributes without null values', () => {
    const activity = issuedBadgeActivity(BASE_BADGE)

    expect(activity.attributes).toEqual({
      badgeTemplateId: BASE_BADGE.badge_template.id,
      badgeTemplateName: 'CKA: Certified Kubernetes Administrator',
      badgeTemplateUrl: BASE_BADGE.badge_template.url,
      badgeTemplateSlug: 'cka-certified-kubernetes-administrator',
      badgeTemplateState: 'active',
      typeCategory: 'Certification',
      level: 'Intermediate',
      skills: ['Kubernetes'],
      globalActivityUrl: BASE_BADGE.badge_template.global_activity_url,
      state: 'accepted',
      issuedAt: '2026-10-19T00:00:00.000Z',
      expiresAt: '2028-10-18T00:00:00.000Z',
      stateUpdatedAt: '2026-10-21T09:18:19.923Z',
      issuerEarnerId: 'LF-kjdz03vcf8',
      imageUrl: 'https://images.credly.com/images/cka.png',
      public: true,
    })
  })

  it.each([
    ['empty', {}],
    ['null', null],
    ['blank id', { id: '   ' }],
  ])(
    'keeps both identities on the email without a sourceId when the user is %s',
    (_label, user) => {
      const activity = issuedBadgeActivity({ ...BASE_BADGE, user, issued_to: null })

      expect(activity.member).toEqual({
        displayName: 'maria.rossi',
        identities: [
          {
            platform: PlatformType.CREDLY,
            value: 'maria.rossi@example.org',
            type: MemberIdentityType.USERNAME,
            verified: true,
          },
          {
            platform: PlatformType.CREDLY,
            value: 'maria.rossi@example.org',
            type: MemberIdentityType.EMAIL,
            verified: true,
          },
        ],
      })
    },
  )

  it('falls back to the canonical badge page when badge_url is missing', () => {
    const activity = issuedBadgeActivity({ ...BASE_BADGE, badge_url: null })

    expect(activity.url).toBe(`https://www.credly.com/badges/${BASE_BADGE.id}`)
  })

  it('normalizes the timestamp to ISO8601 UTC', () => {
    const activity = issuedBadgeActivity({
      ...BASE_BADGE,
      issued_at: '2026-10-19T02:00:00+02:00',
    })

    expect(activity.timestamp).toBe('2026-10-19T00:00:00.000Z')
  })

  it('passes through credlyActivitySchema unchanged', () => {
    const activity = issuedBadgeActivity(BASE_BADGE)

    expect(credlyActivitySchema.parse(activity)).toEqual(activity)
  })
})

describe('stateBadgeActivity', () => {
  it.each([
    ['accepted', CredlyActivityType.ACCEPTED_BADGE, 1],
    ['rejected', CredlyActivityType.REJECTED_BADGE, 0],
    ['revoked', CredlyActivityType.REVOKED_BADGE, 0],
    ['replaced', CredlyActivityType.REPLACED_BADGE, 0],
  ] as const)('maps state %s to %s at state_updated_at', (state, type, score) => {
    const activity = stateBadgeActivity({ ...BASE_BADGE, state })

    expect(activity).toMatchObject({
      type,
      score,
      sourceId: BASE_BADGE.id,
      timestamp: '2026-10-21T09:18:19.923Z',
    })
  })

  it('returns null for a pending badge', () => {
    expect(stateBadgeActivity({ ...BASE_BADGE, state: 'pending' })).toBeNull()
  })

  it('falls back to issued_at when state_updated_at is missing', () => {
    const activity = stateBadgeActivity({
      ...BASE_BADGE,
      state_updated_at: null,
    })

    expect(activity?.timestamp).toBe('2026-10-19T00:00:00.000Z')
  })

  it('keeps the replacement chain in attributes', () => {
    const activity = stateBadgeActivity({
      ...BASE_BADGE,
      state: 'replaced',
      replacement_badge_id: 'c1babec4-3333-4444-8555-666677778888',
      badge_url: null,
      user: {},
    })

    expect(activity?.attributes).toMatchObject({
      state: 'replaced',
      replacementBadgeId: 'c1babec4-3333-4444-8555-666677778888',
    })
    expect(activity?.attributes).not.toHaveProperty('replacesBadgeId')
  })
})

describe('expiredBadgeActivity', () => {
  it('maps an accepted badge with an expiry to expired-badge at expires_at', () => {
    const activity = expiredBadgeActivity(BASE_BADGE)

    expect(activity).toMatchObject({
      type: CredlyActivityType.EXPIRED_BADGE,
      score: 0,
      sourceId: BASE_BADGE.id,
      timestamp: '2028-10-18T00:00:00.000Z',
    })
  })

  it('returns null without an expiry date', () => {
    expect(expiredBadgeActivity({ ...BASE_BADGE, expires_at: null })).toBeNull()
  })

  it.each(['pending', 'rejected', 'revoked', 'replaced'] as const)(
    'returns null for a %s badge',
    (state) => {
      expect(expiredBadgeActivity({ ...BASE_BADGE, state })).toBeNull()
    },
  )
})

describe('badgeUrl', () => {
  it('prefers badge_url over the canonical page', () => {
    expect(badgeUrl(BASE_BADGE)).toBe(BASE_BADGE.badge_url)
    expect(badgeUrl({ id: 'abc', badge_url: undefined })).toBe('https://www.credly.com/badges/abc')
  })
})

describe('toIsoTimestamp', () => {
  it('rejects a string that is not a date', () => {
    expect(() => toIsoTimestamp('yesterday')).toThrow('credly timestamp is not a valid date/time')
  })
})

describe('credlyBadgeSchema', () => {
  it('accepts a badge with an empty user and keeps unknown fields', () => {
    const parsed = credlyBadgeSchema.parse({
      ...BASE_BADGE,
      user: {},
      evidence: [],
    })

    expect(parsed.user).toEqual({})
    expect(parsed).toHaveProperty('evidence', [])
  })

  it('rejects an unknown state', () => {
    expect(() => credlyBadgeSchema.parse({ ...BASE_BADGE, state: 'expired' })).toThrow()
  })

  it('rejects a blank recipient_email', () => {
    expect(() => credlyBadgeSchema.parse({ ...BASE_BADGE, recipient_email: '   ' })).toThrow()
  })
})
