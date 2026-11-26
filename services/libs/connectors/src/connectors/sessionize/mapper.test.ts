import { describe, expect, it } from 'vitest'

import { SessionizeActivityType } from '@crowd/integrations/src/integrations/sessionize/types'
import {
  MemberIdentityType,
  OrganizationIdentityType,
  OrganizationSource,
  PlatformType,
} from '@crowd/types'

import type { SessionizeExportRow } from './exportRows'
import type { SessionizeEventContext } from './mapper'
import {
  linkedinHandle,
  sessionizeActivitySchema,
  spokeAtSessionActivity,
  submittedSessionActivity,
} from './mapper'

const SPEAKER_ID = '4b7e2c1a-9d3f-4e8a-b6c5-0123456789ab'

const EVENT: SessionizeEventContext = {
  eventKey: 'zx81qwer',
  eventName: 'Example Open Source Summit 2026',
  eventStart: '2026-06-10',
  eventEnd: '2026-06-12',
}

const ROW: SessionizeExportRow = {
  sessionId: '700123',
  title: 'Scaling Widgets in Practice',
  status: 'Accepted',
  dateSubmitted: '2026-02-03T14:05:00.000Z',
  speakerId: SPEAKER_ID,
  firstName: 'Avery',
  lastName: 'Quill',
  email: 'avery.quill@widgetco.example',
  linkedin: 'https://www.linkedin.com/in/Avery-Quill-42/',
  bio: 'Builds widgets.',
  tagline: 'Widget engineer',
  photoUrl: 'https://cdn.example.com/avatars/avery.png',
  company: 'WidgetCo',
  companyWebsite: 'https://widgetco.example.com',
  jobTitle: 'Principal Engineer',
  room: 'Hall B',
  scheduledAt: '2026-06-11T09:30:00.000Z',
  durationMinutes: 30,
}

function row(overrides: Partial<SessionizeExportRow> = {}): SessionizeExportRow {
  return { ...ROW, ...overrides }
}

const SPEAKER_IDENTITY = {
  platform: PlatformType.SESSIONIZE,
  type: MemberIdentityType.USERNAME,
  value: SPEAKER_ID,
  verified: true,
}

describe('submittedSessionActivity', () => {
  it('maps a row to a submitted-session activity', () => {
    expect(submittedSessionActivity(ROW, EVENT)).toEqual({
      type: SessionizeActivityType.SUBMITTED_SESSION,
      platform: PlatformType.SESSIONIZE,
      timestamp: '2026-02-03T14:05:00.000Z',
      sourceId: `700123-${SPEAKER_ID}`,
      score: 3,
      title: 'Scaling Widgets in Practice',
      channel: 'zx81qwer',
      attributes: {
        status: 'Accepted',
        sessionTitle: 'Scaling Widgets in Practice',
        company: 'WidgetCo',
        companyWebsite: 'widgetco.example.com',
        jobTitle: 'Principal Engineer',
        eventName: 'Example Open Source Summit 2026',
      },
      member: {
        displayName: 'Avery Quill',
        identities: [
          SPEAKER_IDENTITY,
          {
            platform: PlatformType.SESSIONIZE,
            type: MemberIdentityType.EMAIL,
            value: 'avery.quill@widgetco.example',
            verified: true,
          },
          {
            platform: PlatformType.LINKEDIN,
            type: MemberIdentityType.USERNAME,
            value: 'avery-quill-42',
            verified: false,
          },
        ],
        attributes: {
          avatarUrl: { [PlatformType.SESSIONIZE]: 'https://cdn.example.com/avatars/avery.png' },
          bio: { [PlatformType.SESSIONIZE]: 'Builds widgets.' },
        },
        organizations: [
          {
            displayName: 'WidgetCo',
            source: OrganizationSource.SESSIONIZE,
            identities: [
              {
                platform: PlatformType.SESSIONIZE,
                type: OrganizationIdentityType.PRIMARY_DOMAIN,
                value: 'widgetco.example.com',
                verified: true,
              },
            ],
          },
        ],
      },
    })
  })

  it('drops null attributes and returns null without a submission date', () => {
    const bare = row({ company: null, companyWebsite: null, jobTitle: null, status: 'Declined' })

    expect(submittedSessionActivity(bare, EVENT)?.attributes).toEqual({
      status: 'Declined',
      sessionTitle: 'Scaling Widgets in Practice',
      eventName: 'Example Open Source Summit 2026',
    })
    expect(submittedSessionActivity(row({ dateSubmitted: null }), EVENT)).toBeNull()
  })
})

describe('spokeAtSessionActivity', () => {
  it('maps an accepted row to a spoke-at-session activity with the same sourceId', () => {
    const activity = spokeAtSessionActivity(ROW, EVENT)

    expect(activity).toMatchObject({
      type: SessionizeActivityType.SPOKE_AT_SESSION,
      platform: PlatformType.SESSIONIZE,
      timestamp: '2026-06-11T09:30:00.000Z',
      sourceId: submittedSessionActivity(ROW, EVENT)?.sourceId,
      score: 8,
      title: 'Scaling Widgets in Practice',
      channel: 'zx81qwer',
      attributes: {
        status: 'Accepted',
        sessionTitle: 'Scaling Widgets in Practice',
        company: 'WidgetCo',
        companyWebsite: 'widgetco.example.com',
        jobTitle: 'Principal Engineer',
        eventName: 'Example Open Source Summit 2026',
        room: 'Hall B',
        scheduledAt: '2026-06-11T09:30:00.000Z',
        durationMinutes: 30,
      },
    })
  })

  it.each<[string, Partial<SessionizeExportRow>, string | null]>([
    [
      'an unscheduled accepted row at the event start',
      { scheduledAt: null },
      '2026-06-10T00:00:00.000Z',
    ],
    ['an accepted row of the accepted export', { dateSubmitted: null }, '2026-06-11T09:30:00.000Z'],
    ['a declined row as nothing', { status: 'Declined' }, null],
    ['a waitlisted row as nothing', { status: 'Waitlisted' }, null],
    ['an accept-queue row as nothing', { status: 'Accept Queue' }, null],
  ])('maps %s', (_, overrides, timestamp) => {
    expect(spokeAtSessionActivity(row(overrides), EVENT)?.timestamp ?? null).toBe(timestamp)
  })

  it('leaves out schedule attributes the row does not have', () => {
    const attributes = spokeAtSessionActivity(
      row({ room: null, scheduledAt: null, durationMinutes: null }),
      EVENT,
    )?.attributes

    expect(attributes).not.toHaveProperty('room')
    expect(attributes).not.toHaveProperty('scheduledAt')
    expect(attributes).not.toHaveProperty('durationMinutes')
  })
})

describe('member', () => {
  it.each<[string, Partial<SessionizeExportRow>, string]>([
    ['first name only', { lastName: null }, 'Avery'],
    ['last name only', { firstName: null }, 'Quill'],
    ['the email local part without a name', { firstName: null, lastName: null }, 'avery.quill'],
    [
      'the speaker id without a name or email',
      { firstName: null, lastName: null, email: null },
      SPEAKER_ID,
    ],
  ])('takes the display name from %s', (_, overrides, displayName) => {
    expect(submittedSessionActivity(row(overrides), EVENT)?.member.displayName).toBe(displayName)
  })

  it('keeps only the speaker id identity and no attributes when nothing else is known', () => {
    const member = submittedSessionActivity(
      row({ email: null, linkedin: null, bio: null, tagline: null, photoUrl: null }),
      EVENT,
    )?.member

    expect(member?.identities).toEqual([SPEAKER_IDENTITY])
    expect(member).not.toHaveProperty('attributes')
  })

  it('uses the tagline as bio when the bio is empty', () => {
    expect(submittedSessionActivity(row({ bio: null }), EVENT)?.member.attributes).toEqual({
      avatarUrl: { [PlatformType.SESSIONIZE]: 'https://cdn.example.com/avatars/avery.png' },
      bio: { [PlatformType.SESSIONIZE]: 'Widget engineer' },
    })
  })

  it('puts no name or email in the activity attributes', () => {
    const serialized = JSON.stringify(spokeAtSessionActivity(ROW, EVENT)?.attributes)

    for (const personal of ['Avery', 'Quill', 'avery.quill@widgetco.example']) {
      expect(serialized).not.toContain(personal)
    }
  })
})

describe('organizations', () => {
  it.each<[string, string | null, string | null, string | null]>([
    ['no website', null, null, null],
    ['a public email provider domain', 'https://gmail.com', 'gmail.com', null],
    ['a website that is not a URL', 'not a url', null, null],
    ['an email typed as the website', 'avery.quill@widgetco.example', null, null],
    ['a linkedin company page', 'https://www.linkedin.com/company/widgetco', null, null],
    ['a regional linkedin host', 'uk.linkedin.com/in/avery-quill-42', null, null],
    ['a github profile', 'github.com/avery-quill', null, null],
    ['a link-in-bio page', 'https://linktr.ee/averyquill', null, null],
    ['a sessionize profile', 'https://sessionize.com/avery-quill', null, null],
    ['a short link', 'https://bit.ly/3AvQuil', null, null],
    ['a linkedin short link', 'lnkd.in/averyq', null, null],
    ['a google doc', 'https://docs.google.com/document/d/made-up-doc', null, null],
    ['a personal github pages site', 'https://avery-quill.github.io', null, null],
    ['a hosted blog', 'averyquill.wordpress.com', null, null],
    [
      'a company website with a path',
      'https://widgetco.example.com/en',
      'widgetco.example.com',
      'widgetco.example.com',
    ],
    ['a google product site', 'https://cloud.google.com', 'cloud.google.com', 'cloud.google.com'],
  ])(
    'maps %s (%s) to the website attribute %s and organization domain %s',
    (_, companyWebsite, attribute, organizationDomain) => {
      const activity = submittedSessionActivity(row({ companyWebsite }), EVENT)

      expect(activity?.attributes.companyWebsite).toBe(attribute ?? undefined)
      expect(activity?.member.organizations?.[0]?.identities[0]?.value).toBe(
        organizationDomain ?? undefined,
      )
    },
  )

  it('keys the organization on the hostname and names it after it when the company is empty', () => {
    const activity = submittedSessionActivity(
      row({ company: null, companyWebsite: 'https://www.WidgetCo.example.com/about?ref=talk' }),
      EVENT,
    )

    expect(activity?.attributes.companyWebsite).toBe('widgetco.example.com')
    expect(activity?.member.organizations).toEqual([
      {
        displayName: 'widgetco.example.com',
        source: OrganizationSource.SESSIONIZE,
        identities: [
          {
            platform: PlatformType.SESSIONIZE,
            type: OrganizationIdentityType.PRIMARY_DOMAIN,
            value: 'widgetco.example.com',
            verified: true,
          },
        ],
      },
    ])
  })
})

describe('linkedinHandle', () => {
  it.each<[string | null, string | null]>([
    ['https://www.linkedin.com/in/Jane-Doe-123/?utm=x', 'jane-doe-123'],
    ['https://de.linkedin.com/in/jane', 'jane'],
    ['linkedin.com/in/jane', 'jane'],
    ['https://www.linkedin.com/company/acme', null],
    ['https://example.com/in/jane', null],
    ['https://notlinkedin.com/in/jane', null],
    ['https://www.linkedin.com/in/', null],
    ['not a url', null],
    [null, null],
  ])('reads %s as %s', (raw, handle) => {
    expect(linkedinHandle(raw)).toBe(handle)
  })
})

describe('sessionizeActivitySchema', () => {
  it.each<[string, Partial<SessionizeExportRow>]>([
    ['a full row', {}],
    [
      'a bare row',
      {
        firstName: null,
        lastName: null,
        email: null,
        linkedin: null,
        bio: null,
        tagline: null,
        photoUrl: null,
        company: null,
        companyWebsite: null,
        jobTitle: null,
        room: null,
        scheduledAt: null,
        durationMinutes: null,
      },
    ],
  ])('accepts every activity mapped from %s', (_, overrides) => {
    const activities = [
      submittedSessionActivity(row(overrides), EVENT),
      spokeAtSessionActivity(row(overrides), EVENT),
    ]

    for (const activity of activities) {
      expect(activity).not.toBeNull()
      expect(sessionizeActivitySchema.parse(activity)).toEqual(activity)
    }
  })
})
