import { z } from 'zod'

import { getEmailLocalPart, isDomainExcluded, normalizeHostname } from '@crowd/common'
import {
  SESSIONIZE_GRID,
  SessionizeActivityType,
} from '@crowd/integrations/src/integrations/sessionize/types'
import {
  MemberAttributeName,
  MemberIdentityType,
  OrganizationIdentityType,
  OrganizationSource,
  PlatformType,
} from '@crowd/types'

import type { SessionizeExportRow } from './exportRows'

export interface SessionizeEventContext {
  eventKey: string
  eventName: string
  eventStart: string
  eventEnd: string
}

const sessionizeIdentitySchema = z.object({
  platform: z.literal(PlatformType.SESSIONIZE),
  type: z.enum([MemberIdentityType.USERNAME, MemberIdentityType.EMAIL]),
  value: z.string().min(1),
  verified: z.literal(true),
})

const linkedinIdentitySchema = z.object({
  platform: z.literal(PlatformType.LINKEDIN),
  type: z.literal(MemberIdentityType.USERNAME),
  value: z.string().min(1),
  verified: z.literal(false),
})

const organizationSchema = z.object({
  displayName: z.string().min(1),
  source: z.literal(OrganizationSource.SESSIONIZE),
  identities: z
    .array(
      z.object({
        platform: z.literal(PlatformType.SESSIONIZE),
        type: z.literal(OrganizationIdentityType.PRIMARY_DOMAIN),
        value: z.string().min(1),
        verified: z.literal(true),
      }),
    )
    .min(1),
})

const memberSchema = z.object({
  displayName: z.string().min(1),
  identities: z.array(z.union([sessionizeIdentitySchema, linkedinIdentitySchema])).min(1),
  attributes: z.record(z.unknown()).optional(),
  organizations: z.array(organizationSchema).min(1).optional(),
})

export const sessionizeActivitySchema = z.object({
  type: z.nativeEnum(SessionizeActivityType),
  platform: z.literal(PlatformType.SESSIONIZE),
  timestamp: z.string().datetime(),
  sourceId: z.string().min(1),
  score: z.number(),
  title: z.string().min(1),
  channel: z.string().min(1),
  attributes: z.record(z.unknown()),
  member: memberSchema,
})

export type SessionizeActivity = z.infer<typeof sessionizeActivitySchema>

type SessionizeMember = SessionizeActivity['member']

const LINKEDIN_HOST = 'linkedin.com'
const URL_SCHEME = /^[a-z][a-z0-9+.-]*:\/\//i

export function submittedSessionActivity(
  row: SessionizeExportRow,
  event: SessionizeEventContext,
): SessionizeActivity | null {
  if (row.dateSubmitted === null) return null
  return sessionActivity(
    row,
    event,
    SessionizeActivityType.SUBMITTED_SESSION,
    row.dateSubmitted,
    sessionAttributes(row, event),
  )
}

export function spokeAtSessionActivity(
  row: SessionizeExportRow,
  event: SessionizeEventContext,
): SessionizeActivity | null {
  if (row.status !== 'Accepted') return null
  return sessionActivity(
    row,
    event,
    SessionizeActivityType.SPOKE_AT_SESSION,
    row.scheduledAt ?? `${event.eventStart}T00:00:00.000Z`,
    {
      ...sessionAttributes(row, event),
      ...withoutNullish({
        room: row.room,
        scheduledAt: row.scheduledAt,
        durationMinutes: row.durationMinutes,
      }),
    },
  )
}

export function linkedinHandle(raw: string | null): string | null {
  if (raw === null) return null
  const url = parseUrl(URL_SCHEME.test(raw) ? raw : `https://${raw}`)
  if (url === null) return null
  const host = url.hostname.toLowerCase()
  if (host !== LINKEDIN_HOST && !host.endsWith(`.${LINKEDIN_HOST}`)) return null
  const [section, handle] = url.pathname.split('/').filter(Boolean)
  return section === 'in' && handle ? handle.toLowerCase() : null
}

function parseUrl(raw: string): URL | null {
  try {
    return new URL(raw)
  } catch {
    return null
  }
}

function sessionActivity(
  row: SessionizeExportRow,
  event: SessionizeEventContext,
  type: SessionizeActivityType,
  timestamp: string,
  attributes: Record<string, unknown>,
): SessionizeActivity {
  return {
    type,
    platform: PlatformType.SESSIONIZE,
    timestamp,
    sourceId: `${row.sessionId}-${row.speakerId}`,
    score: SESSIONIZE_GRID[type].score,
    title: row.title,
    channel: event.eventKey,
    attributes,
    member: speakerMember(row),
  }
}

function sessionAttributes(
  row: SessionizeExportRow,
  event: SessionizeEventContext,
): Record<string, unknown> {
  return withoutNullish({
    status: row.status,
    sessionTitle: row.title,
    company: row.company,
    companyWebsite: row.companyWebsite,
    jobTitle: row.jobTitle,
    eventName: event.eventName,
  })
}

function speakerMember(row: SessionizeExportRow): SessionizeMember {
  const bio = row.bio ?? row.tagline
  const attributes = withoutNullish({
    [MemberAttributeName.AVATAR_URL]: row.photoUrl
      ? { [PlatformType.SESSIONIZE]: row.photoUrl }
      : null,
    [MemberAttributeName.BIO]: bio ? { [PlatformType.SESSIONIZE]: bio } : null,
  })
  const organizations = speakerOrganizations(row)
  return {
    displayName: speakerDisplayName(row),
    identities: speakerIdentities(row),
    ...(Object.keys(attributes).length > 0 && { attributes }),
    ...(organizations && { organizations }),
  }
}

function speakerDisplayName(row: SessionizeExportRow): string {
  const name = [row.firstName, row.lastName].filter(Boolean).join(' ')
  return name || (row.email && getEmailLocalPart(row.email)) || row.speakerId
}

function speakerIdentities(row: SessionizeExportRow): SessionizeMember['identities'] {
  const handle = linkedinHandle(row.linkedin)
  return [
    {
      platform: PlatformType.SESSIONIZE,
      type: MemberIdentityType.USERNAME,
      value: row.speakerId,
      verified: true,
    },
    ...(row.email
      ? [
          {
            platform: PlatformType.SESSIONIZE,
            type: MemberIdentityType.EMAIL,
            value: row.email,
            verified: true,
          } as const,
        ]
      : []),
    ...(handle
      ? [
          {
            platform: PlatformType.LINKEDIN,
            type: MemberIdentityType.USERNAME,
            value: handle,
            verified: false,
          } as const,
        ]
      : []),
  ]
}

function speakerOrganizations(row: SessionizeExportRow): SessionizeMember['organizations'] {
  const website = row.companyWebsite
  if (website === null) return undefined
  const hostname = normalizeHostname(website, false)
  if (hostname === undefined || isDomainExcluded(hostname)) return undefined
  return [
    {
      displayName: row.company ?? website,
      source: OrganizationSource.SESSIONIZE,
      identities: [
        {
          platform: PlatformType.SESSIONIZE,
          type: OrganizationIdentityType.PRIMARY_DOMAIN,
          value: website,
          verified: true,
        },
      ],
    },
  ]
}

function withoutNullish(obj: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(obj).filter(([, v]) => v !== null && v !== undefined))
}
