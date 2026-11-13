import moment from 'moment-timezone'

import { isValidEmail } from '@crowd/common'
import { SCHED_GRID, SchedActivityType } from '@crowd/integrations/src/integrations/sched/types'
import {
  IActivityData,
  IMemberData,
  MemberAttributeName,
  MemberIdentityType,
  PlatformType,
} from '@crowd/types'

import type {
  SchedEventData,
  SchedRegistration,
  SchedSession,
  SchedSpeaker,
  SchedUser,
} from './api'

export interface SchedEvent {
  subdomain: string
  name: string
  segment: { slug: string; sourceId: string }
}

export type RegistrationTime = 'session-start' | 'last-update'

export interface TransformOptions {
  registrationTime: RegistrationTime
  emailsVerified: boolean
}

export const DEFAULT_TRANSFORM_OPTIONS: TransformOptions = {
  registrationTime: 'session-start',
  emailsVerified: true,
}

export interface EventActivities {
  activities: IActivityData[]
  skipped: {
    onCancelledSessions: number
    onUnknownSessions: number
    withoutTimestamp: number
  }
}

const SESSION_START_FORMAT = 'YYYY-MM-DD HH:mm:ss'
const LAST_UPDATE_FORMAT = 'MM-DD-YYYY HH:mm'
const CANCELLED_PREFIX = '[cancelled]'

function toUtc(value: string | undefined, format: string, timezone: string): string | null {
  if (!value) {
    return null
  }
  const time = moment.tz(value, format, true, timezone)
  return time.isValid() ? time.toISOString() : null
}

function isCancelled(session: SchedSession): boolean {
  return session.name.toLowerCase().startsWith(CANCELLED_PREFIX)
}

function cleanEmail(email: string | undefined): string | null {
  const value = email?.trim().toLowerCase()
  return value && isValidEmail(value) ? value : null
}

function buildMember(
  username: string,
  user: SchedUser | undefined,
  fallback: { name?: string; company?: string; position?: string },
  options: TransformOptions,
): IMemberData {
  const identities: IMemberData['identities'] = [
    {
      platform: PlatformType.SCHED,
      value: username,
      type: MemberIdentityType.USERNAME,
      verified: true,
      verifiedBy: PlatformType.SCHED,
    },
  ]
  const email = cleanEmail(user?.email)
  if (email) {
    identities.push({
      platform: PlatformType.SCHED,
      value: email,
      type: MemberIdentityType.EMAIL,
      verified: options.emailsVerified,
      ...(options.emailsVerified && { verifiedBy: PlatformType.SCHED }),
    })
  }

  const attributes: IMemberData['attributes'] = {}
  const setAttribute = (name: MemberAttributeName, value: string | undefined) => {
    if (value) {
      attributes[name] = { [PlatformType.SCHED]: value }
    }
  }
  setAttribute(MemberAttributeName.COMPANY, user?.company ?? fallback.company)
  setAttribute(MemberAttributeName.JOB_TITLE, user?.position ?? fallback.position)
  setAttribute(MemberAttributeName.LOCATION, user?.location)
  setAttribute(MemberAttributeName.AVATAR_URL, user?.avatar)

  return {
    displayName: user?.name ?? fallback.name ?? username,
    identities,
    attributes,
  }
}

function sessionFields(event: SchedEvent, session: SchedSession, sessionStart: string) {
  return {
    platform: PlatformType.SCHED,
    channel: event.subdomain,
    url: `https://${event.subdomain}.sched.com/event/${session.id}`,
    attributes: {
      eventName: event.name,
      sessionName: session.name,
      sessionType: session.type ?? null,
      sessionStart,
      venue: session.venue ?? null,
    },
  }
}

export function buildEventActivities(
  event: SchedEvent,
  data: SchedEventData,
  options: TransformOptions = DEFAULT_TRANSFORM_OPTIONS,
): EventActivities {
  const result: EventActivities = {
    activities: [],
    skipped: { onCancelledSessions: 0, onUnknownSessions: 0, withoutTimestamp: 0 },
  }
  const usersByName = new Map(data.users.map((user) => [user.username, user]))
  const sessionsByKey = new Map(data.sessions.map((session) => [session.key, session]))
  const sessionsById = new Map(data.sessions.map((session) => [session.id, session]))

  const usableSession = (session: SchedSession | undefined): SchedSession | null => {
    if (!session) {
      result.skipped.onUnknownSessions++
      return null
    }
    if (isCancelled(session)) {
      result.skipped.onCancelledSessions++
      return null
    }
    return session
  }

  const registrationActivity = (registration: SchedRegistration): IActivityData | null => {
    const session = usableSession(sessionsByKey.get(registration.sessionKey))
    if (!session) {
      return null
    }
    const sessionStart = toUtc(session.start, SESSION_START_FORMAT, data.timezone)
    const timestamp =
      options.registrationTime === 'session-start'
        ? sessionStart
        : toUtc(registration.updated, LAST_UPDATE_FORMAT, data.timezone)
    if (!sessionStart || !timestamp) {
      result.skipped.withoutTimestamp++
      return null
    }
    return {
      ...sessionFields(event, session, sessionStart),
      type: SchedActivityType.REGISTERED_SESSION,
      score: SCHED_GRID[SchedActivityType.REGISTERED_SESSION].score,
      timestamp,
      sourceId: `${event.subdomain}:${registration.username}:${session.key}`,
      sourceParentId: `${event.subdomain}:${session.key}`,
      username: registration.username,
      member: buildMember(
        registration.username,
        usersByName.get(registration.username),
        registration,
        options,
      ),
    }
  }

  const speakerActivities = (speaker: SchedSpeaker): IActivityData[] =>
    speaker.sessionIds.flatMap((sessionId) => {
      const session = usableSession(sessionsById.get(sessionId))
      if (!session) {
        return []
      }
      const sessionStart = toUtc(session.start, SESSION_START_FORMAT, data.timezone)
      if (!sessionStart) {
        result.skipped.withoutTimestamp++
        return []
      }
      return [
        {
          ...sessionFields(event, session, sessionStart),
          type: SchedActivityType.SPOKE_AT_SESSION,
          score: SCHED_GRID[SchedActivityType.SPOKE_AT_SESSION].score,
          timestamp: sessionStart,
          sourceId: `${event.subdomain}:speaker:${speaker.username}:${session.id}`,
          sourceParentId: `${event.subdomain}:${session.id}`,
          username: speaker.username,
          member: buildMember(
            speaker.username,
            usersByName.get(speaker.username),
            speaker,
            options,
          ),
        },
      ]
    })

  for (const registration of data.registrations) {
    const activity = registrationActivity(registration)
    if (activity) {
      result.activities.push(activity)
    }
  }
  for (const speaker of data.speakers) {
    result.activities.push(...speakerActivities(speaker))
  }
  return result
}
