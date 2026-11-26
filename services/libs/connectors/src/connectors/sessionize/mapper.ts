import { z } from 'zod'

import {
  getEmailLocalPart,
  isDomainExcluded,
  isValidEmail,
  normalizeHostname,
  privateSuffix,
  registrableDomain,
} from '@crowd/common'
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
  value: z.string().refine((value) => isValidEmail(value)),
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
  identities: z.array(z.union([sessionizeIdentitySchema, linkedinIdentitySchema])).min(2),
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
const NOT_A_LINKEDIN_HANDLE = /[\s\p{Cc}/]/u
const SHARED_HOSTS = [
  LINKEDIN_HOST,
  'github.com',
  'gitlab.com',
  'twitter.com',
  'x.com',
  'facebook.com',
  'instagram.com',
  'youtube.com',
  'medium.com',
  'linktr.ee',
  'sessionize.com',
  'bit.ly',
  'tinyurl.com',
  't.co',
  'lnkd.in',
  'youtu.be',
  't.me',
  'bio.link',
  'about.me',
  'beacons.ai',
  'carrd.co',
  'calendly.com',
  'dev.to',
  'substack.com',
  'meetup.com',
  'bsky.app',
  'mastodon.social',
  'huggingface.co',
  'sites.google.com',
  'docs.google.com',
  'drive.google.com',
  'notion.so',
  'wordpress.com',
  'notion.site',
  'hashnode.dev',
]
const REGISTRY_SUFFIXES = new Set([
  'uk.com',
  'us.com',
  'eu.com',
  'de.com',
  'co.com',
  'za.com',
  'br.com',
  'cn.com',
  'gb.net',
  'uk.net',
  'in.net',
  'it.com',
  'co.nl',
  'co.no',
  'co.dk',
  'co.ca',
  'co.pl',
  'co.cz',
  'com.de',
  'com.se',
  'co.ua',
  'eu.org',
])
const URL_SCHEME = /^[a-z][a-z0-9+.-]*:\/\//i
const WHOLE_EMAIL = /^[^@\s/]+@[^@\s/]+$/

export function submittedSessionActivity(
  row: SessionizeExportRow,
  event: SessionizeEventContext,
): SessionizeActivity | null {
  const email = speakerEmail(row)
  if (row.dateSubmitted === null || email === null) return null
  return sessionActivity(
    row,
    email,
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
  const email = speakerEmail(row)
  if (row.status !== 'Accepted' || email === null) return null
  return sessionActivity(
    row,
    email,
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

export function hasSpeakerEmail(row: SessionizeExportRow): boolean {
  return speakerEmail(row) !== null
}

function speakerEmail(row: SessionizeExportRow): string | null {
  return row.email !== null && isValidEmail(row.email) ? row.email : null
}

export function linkedinHandle(raw: string | null): string | null {
  if (raw === null) return null
  const url = parseUrl(withScheme(raw))
  if (url === null) return null
  const host = url.hostname.toLowerCase()
  if (host !== LINKEDIN_HOST && !host.endsWith(`.${LINKEDIN_HOST}`)) return null
  const [section, handle] = url.pathname.split('/').filter(Boolean)
  return section === 'in' && handle ? decodePathSegment(handle) : null
}

function decodePathSegment(segment: string): string | null {
  try {
    const decoded = decodeURIComponent(segment)
    return decoded === '' || NOT_A_LINKEDIN_HANDLE.test(decoded) ? null : decoded
  } catch {
    return null
  }
}

function withScheme(raw: string): string {
  return URL_SCHEME.test(raw) ? raw : `https://${raw}`
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
  email: string,
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
    member: speakerMember(row, email),
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
    companyWebsite: companyHostname(row.companyWebsite),
    jobTitle: row.jobTitle,
    eventName: event.eventName,
  })
}

function speakerMember(row: SessionizeExportRow, email: string): SessionizeMember {
  const bio = row.bio ?? row.tagline
  const attributes = withoutNullish({
    [MemberAttributeName.AVATAR_URL]: row.photoUrl
      ? { [PlatformType.SESSIONIZE]: row.photoUrl }
      : null,
    [MemberAttributeName.BIO]: bio ? { [PlatformType.SESSIONIZE]: bio } : null,
  })
  const organizations = speakerOrganizations(row)
  return {
    displayName: speakerDisplayName(row, email),
    identities: speakerIdentities(row, email),
    ...(Object.keys(attributes).length > 0 && { attributes }),
    ...(organizations && { organizations }),
  }
}

function speakerDisplayName(row: SessionizeExportRow, email: string): string {
  const name = [row.firstName, row.lastName].filter(Boolean).join(' ')
  return name || getEmailLocalPart(email)
}

function speakerIdentities(
  row: SessionizeExportRow,
  email: string,
): SessionizeMember['identities'] {
  const handle = linkedinHandle(row.linkedin)
  return [
    {
      platform: PlatformType.SESSIONIZE,
      type: MemberIdentityType.USERNAME,
      value: email,
      verified: true,
    },
    {
      platform: PlatformType.SESSIONIZE,
      type: MemberIdentityType.EMAIL,
      value: email,
      verified: true,
    },
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

function isPersonalPageHost(hostname: string): boolean {
  const suffix = privateSuffix(hostname)
  return suffix !== null && (hostname === suffix || !REGISTRY_SUFFIXES.has(suffix))
}

function isSharedHost(hostname: string): boolean {
  return (
    isPersonalPageHost(hostname) ||
    SHARED_HOSTS.some((shared) => hostname === shared || hostname.endsWith(`.${shared}`))
  )
}

function isEmailOrUserinfo(website: string): boolean {
  if (!URL_SCHEME.test(website) && WHOLE_EMAIL.test(website)) return true
  const url = parseUrl(withScheme(website))
  return url !== null && (url.username !== '' || url.password !== '')
}

function isEmailProviderHost(hostname: string): boolean {
  return isDomainExcluded(hostname) || isDomainExcluded(registrableDomain(hostname) ?? hostname)
}

function companyHostname(website: string | null): string | null {
  if (website === null || isEmailOrUserinfo(website)) return null
  const hostname = normalizeHostname(website, false)
  return hostname === undefined || isSharedHost(hostname) ? null : hostname
}

function speakerOrganizations(row: SessionizeExportRow): SessionizeMember['organizations'] {
  const hostname = companyHostname(row.companyWebsite)
  if (hostname === null || isEmailProviderHost(hostname)) return undefined
  return [
    {
      displayName: row.company ?? hostname,
      source: OrganizationSource.SESSIONIZE,
      identities: [
        {
          platform: PlatformType.SESSIONIZE,
          type: OrganizationIdentityType.PRIMARY_DOMAIN,
          value: hostname,
          verified: true,
        },
      ],
    },
  ]
}

function withoutNullish(obj: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(obj).filter(([, v]) => v !== null && v !== undefined))
}
