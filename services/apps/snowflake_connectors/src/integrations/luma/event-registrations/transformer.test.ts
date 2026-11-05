import { describe, expect, it, vi } from 'vitest'

import { MemberIdentityType, OrganizationIdentityType, PlatformType } from '@crowd/types'

import { LumaEventRegistrationsTransformer } from './transformer'

vi.mock('@crowd/integrations', () => ({
  LumaActivityType: { REGISTERED_EVENT: 'registered-event', ATTENDED_EVENT: 'attended-event' },
  LUMA_GRID: { 'registered-event': { score: 3 }, 'attended-event': { score: 8 } },
}))

const REGISTERED_EVENT = 'registered-event'
const ATTENDED_EVENT = 'attended-event'

const transformer = new LumaEventRegistrationsTransformer()

const baseRow = {
  REGISTRATION_ID: 'reg-1',
  REGISTRATION_STATUS: 'Accepted',
  REGISTRATION_TYPE: 'paid',
  REGISTRATION_TS: new Date('2026-10-20T16:30:00.000Z'),
  USER_ATTENDED: false,
  USER_CHECKED_IN_TS: null,
  EVENT_ID: 'gatewaze_event-1',
  EVENT_NAME: 'Voice Agents Forum',
  EVENT_START_DATE: new Date('2026-11-05T00:00:00.000Z'),
  EVENT_URL: 'https://luma.com/abc123',
  EVENT_CITY: 'San Francisco',
  EVENT_COUNTRY: 'United States',
  EVENT_REGISTRATION_TYPE: 'In Person',
  PROJECT_ID: 'lfZA9uadMdkwBqqHsj',
  PROJECT_SLUG: 'agentic-ai-foundation',
  EMAIL: 'Jane.Doe@acme.io',
  FULL_NAME: ' Jane Doe ',
  JOB_TITLE: 'Engineer',
  USER_PHOTO_URL: null,
  USER_COUNTRY: null,
  ACCOUNT_NAME: 'Acme',
  ORG_WEBSITE: 'acme.io',
  ORG_DOMAIN_ALIASES: 'acme.com, ,acme.dev',
  LFID: null,
}

describe('LumaEventRegistrationsTransformer', () => {
  it('turns an accepted registration into one registered-event with an unverified email', () => {
    const results = transformer.transformRow({ ...baseRow, USER_ATTENDED: true })

    expect(results).toHaveLength(1)
    const [{ activity, segment }] = results ?? []
    expect(segment).toEqual({ slug: 'agentic-ai-foundation', sourceId: 'lfZA9uadMdkwBqqHsj' })
    expect(activity).toMatchObject({
      type: REGISTERED_EVENT,
      platform: PlatformType.LUMA,
      timestamp: '2026-10-20T16:30:00.000Z',
      score: 3,
      sourceId: 'reg-1',
      sourceParentId: 'gatewaze_event-1',
      url: 'https://luma.com/abc123',
      attributes: {
        eventName: 'Voice Agents Forum',
        eventDate: '2026-11-05',
        location: 'San Francisco, United States',
        website: 'https://luma.com/abc123',
        registrationType: 'paid',
        eventType: 'In Person',
      },
    })
    expect(activity.member.displayName).toBe('Jane Doe')
    expect(activity.member.identities).toEqual([
      {
        platform: PlatformType.LUMA,
        value: 'Jane.Doe@acme.io',
        type: MemberIdentityType.USERNAME,
        verified: true,
        verifiedBy: PlatformType.LUMA,
      },
      {
        platform: PlatformType.LUMA,
        value: 'Jane.Doe@acme.io',
        type: MemberIdentityType.EMAIL,
        verified: false,
      },
    ])
    expect(activity.member.organizations?.[0].identities.map((i) => [i.type, i.value])).toEqual([
      [OrganizationIdentityType.PRIMARY_DOMAIN, 'acme.io'],
      [OrganizationIdentityType.ALTERNATIVE_DOMAIN, 'acme.com'],
      [OrganizationIdentityType.ALTERNATIVE_DOMAIN, 'acme.dev'],
    ])
  })

  it('adds an attended-event at the check-in time with the same source id and its own member copy', () => {
    const results = transformer.transformRow({
      ...baseRow,
      LFID: 'janedoe',
      USER_ATTENDED: true,
      USER_CHECKED_IN_TS: '2026-11-05 17:05:00',
      ORG_WEBSITE: null,
      ORG_DOMAIN_ALIASES: null,
    })

    expect(
      results?.map(({ activity }) => [activity.type, activity.timestamp, activity.score]),
    ).toEqual([
      [REGISTERED_EVENT, '2026-10-20T16:30:00.000Z', 3],
      [ATTENDED_EVENT, '2026-11-05T17:05:00.000Z', 8],
    ])
    const [registered, attended] = results ?? []
    expect(attended.activity.sourceId).toBe(registered.activity.sourceId)
    expect(attended.activity.member).not.toBe(registered.activity.member)
    expect(attended.activity.member.identities).not.toBe(registered.activity.member.identities)
    expect(attended.activity.member.organizations).toBeUndefined()
    expect(
      attended.activity.member.identities.map((i) => [i.platform, i.type, i.value, i.verified]),
    ).toEqual([
      [PlatformType.LFID, MemberIdentityType.USERNAME, 'janedoe', true],
      [PlatformType.LUMA, MemberIdentityType.USERNAME, 'janedoe', true],
      [PlatformType.LUMA, MemberIdentityType.EMAIL, 'Jane.Doe@acme.io', false],
    ])
  })

  it('emits only the attended-event for a check-in whose registration is not accepted', () => {
    const results = transformer.transformRow({
      ...baseRow,
      REGISTRATION_STATUS: 'Pending Approval',
      USER_ATTENDED: true,
      USER_CHECKED_IN_TS: new Date('2026-11-05T17:05:00.000Z'),
    })

    expect(results?.map(({ activity }) => activity.type)).toEqual([ATTENDED_EVENT])
  })
})
