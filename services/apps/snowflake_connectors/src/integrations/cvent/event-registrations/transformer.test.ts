import { describe, expect, it, vi } from 'vitest'

import { MemberIdentityType, PlatformType } from '@crowd/types'

import { CventTransformer } from './transformer'

vi.mock('@crowd/integrations', () => ({
  CventActivityType: { ATTENDED_EVENT: 'attended-event', REGISTERED_EVENT: 'registered-event' },
  CVENT_GRID: { 'attended-event': { score: 1 }, 'registered-event': { score: 1 } },
}))

const registration = (overrides: Record<string, unknown>): Record<string, unknown> => ({
  REGISTRATION_ID: 'reg-1',
  EVENT_ID: 'event-1',
  EVENT_NAME: 'Open Source Summit',
  PROJECT_SLUG: 'pytorch',
  PROJECT_ID: 'project-1',
  FULL_NAME: 'Ada Attendee',
  EMAIL: 'ada@vates.tech',
  USER_ID: 'lfD2FiXsiwkYJBZT53',
  USER_NAME: 'registrant',
  ...overrides,
})

const identitiesOf = (row: Record<string, unknown>) => {
  const [result] = new CventTransformer().safeTransformRow(row) ?? []
  return result.activity.member.identities.map(({ platform, type, value, sourceId }) => ({
    platform,
    type,
    value,
    sourceId,
  }))
}

describe('CventTransformer', () => {
  it.each([
    ['the attendee has no LFID', { LFID: null, REGISTRANT_LFID: 'registrant' }],
    ['the attendee has another LFID', { LFID: 'ada', REGISTRANT_LFID: 'registrant' }],
  ])('keeps the registrant identities off a group registration when %s', (_case, overrides) => {
    const identities = identitiesOf(registration(overrides))
    const attendeeLfid = overrides.LFID

    expect(identities).toEqual([
      {
        platform: PlatformType.CVENT,
        type: MemberIdentityType.EMAIL,
        value: 'ada@vates.tech',
        sourceId: undefined,
      },
      ...(attendeeLfid
        ? [
            {
              platform: PlatformType.LFID,
              type: MemberIdentityType.USERNAME,
              value: attendeeLfid,
              sourceId: undefined,
            },
          ]
        : []),
      {
        platform: PlatformType.CVENT,
        type: MemberIdentityType.USERNAME,
        value: attendeeLfid ?? 'ada@vates.tech',
        sourceId: undefined,
      },
    ])
  })

  it('keeps the registrant identities when the registrant is the attendee', () => {
    const identities = identitiesOf(
      registration({ LFID: 'Registrant', REGISTRANT_LFID: 'registrant' }),
    )

    expect(identities).toEqual([
      {
        platform: PlatformType.CVENT,
        type: MemberIdentityType.EMAIL,
        value: 'ada@vates.tech',
        sourceId: 'lfD2FiXsiwkYJBZT53',
      },
      {
        platform: PlatformType.CVENT,
        type: MemberIdentityType.USERNAME,
        value: 'registrant',
        sourceId: 'lfD2FiXsiwkYJBZT53',
      },
      {
        platform: PlatformType.LFID,
        type: MemberIdentityType.USERNAME,
        value: 'Registrant',
        sourceId: 'lfD2FiXsiwkYJBZT53',
      },
    ])
  })
})
