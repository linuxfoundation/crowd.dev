import { describe, expect, it } from 'vitest'

import { MemberIdentityType, PlatformType } from '@crowd/types'

import type { SchedEventData } from './api'
import { SchedEvent, buildEventActivities } from './transform'

const event: SchedEvent = {
  subdomain: 'testevent2021',
  name: 'Test Event 2021',
  segment: { slug: 'test-project', sourceId: 'src-1' },
}

const SESSION_ID = '0123456789abcdef0123456789abcdef'

function eventData(overrides: Partial<SchedEventData> = {}): SchedEventData {
  return {
    timezone: 'Europe/Brussels',
    sessions: [
      {
        id: SESSION_ID,
        key: '297',
        name: 'Keynote',
        start: '2021-05-04 14:00:00',
        type: 'Keynote',
      },
      {
        id: 'f'.repeat(32),
        key: '298',
        name: '[CANCELLED] Lightning talk',
        start: '2021-05-04 15:00:00',
      },
    ],
    users: [
      {
        username: 'jdoe',
        name: 'Jane Doe',
        email: ' Jane.Doe@Example.org ',
        company: 'Acme',
        position: 'Engineer',
      },
    ],
    registrations: [{ username: 'jdoe', sessionKey: '297', updated: '11-02-2023 09:30' }],
    speakers: [{ username: 'jdoe', sessionIds: [SESSION_ID] }],
    ...overrides,
  }
}

describe('buildEventActivities', () => {
  it('builds ids that include the event, with the subdomain as channel', () => {
    const [registration, speaker] = buildEventActivities(event, eventData()).activities

    expect(registration).toMatchObject({
      type: 'registered-session',
      platform: PlatformType.SCHED,
      channel: 'testevent2021',
      sourceId: 'testevent2021:jdoe:297',
      sourceParentId: 'testevent2021:297',
      url: `https://testevent2021.sched.com/event/${SESSION_ID}`,
      score: 3,
    })
    expect(speaker).toMatchObject({
      type: 'spoke-at-session',
      channel: 'testevent2021',
      sourceId: `testevent2021:speaker:jdoe:${SESSION_ID}`,
      sourceParentId: `testevent2021:${SESSION_ID}`,
      score: 8,
    })
  })

  it('reads session times in the event timezone', () => {
    const { activities } = buildEventActivities(event, eventData())

    expect(activities.map((activity) => activity.timestamp)).toEqual([
      '2021-05-04T12:00:00.000Z',
      '2021-05-04T12:00:00.000Z',
    ])
  })

  it('uses the last update as registration time when asked', () => {
    const [registration] = buildEventActivities(event, eventData(), {
      registrationTime: 'last-update',
      emailsVerified: true,
    }).activities

    expect(registration.timestamp).toBe('2023-11-02T08:30:00.000Z')
  })

  it('skips cancelled and unknown sessions', () => {
    const result = buildEventActivities(
      event,
      eventData({
        registrations: [
          { username: 'jdoe', sessionKey: '298' },
          { username: 'jdoe', sessionKey: '999' },
        ],
        speakers: [{ username: 'jdoe', sessionIds: ['missing'] }],
      }),
    )

    expect(result.activities).toEqual([])
    expect(result.skipped).toEqual({
      onCancelledSessions: 1,
      onUnknownSessions: 2,
      withoutTimestamp: 0,
    })
  })

  it('adds the email as a verified identity when the key returns it', () => {
    const [registration] = buildEventActivities(event, eventData()).activities

    expect(registration.member?.displayName).toBe('Jane Doe')
    expect(registration.member?.identities).toEqual([
      {
        platform: PlatformType.SCHED,
        value: 'jdoe',
        type: MemberIdentityType.USERNAME,
        verified: true,
        verifiedBy: PlatformType.SCHED,
      },
      {
        platform: PlatformType.SCHED,
        value: 'jane.doe@example.org',
        type: MemberIdentityType.EMAIL,
        verified: true,
        verifiedBy: PlatformType.SCHED,
      },
    ])
  })

  it('keeps only the username without an email, using registration fields as fallback', () => {
    const [registration] = buildEventActivities(
      event,
      eventData({
        users: [],
        registrations: [
          { username: 'guest', name: 'Guest', sessionKey: '297', company: 'Initech' },
        ],
        speakers: [],
      }),
    ).activities

    expect(registration.member).toEqual({
      displayName: 'Guest',
      identities: [
        {
          platform: PlatformType.SCHED,
          value: 'guest',
          type: MemberIdentityType.USERNAME,
          verified: true,
          verifiedBy: PlatformType.SCHED,
        },
      ],
      attributes: { company: { sched: 'Initech' } },
    })
  })
})
