import moment from 'moment-timezone'

import { SchedCallError, callSched } from './client'

export interface SchedSession {
  id: string
  key: string
  name: string
  start: string
  type?: string
  venue?: string
}

export interface SchedUser {
  username: string
  name?: string
  email?: string
  company?: string
  position?: string
  location?: string
  avatar?: string
}

export interface SchedRegistration {
  username: string
  name?: string
  sessionKey: string
  updated?: string
  company?: string
  position?: string
}

export interface SchedSpeaker {
  username: string
  name?: string
  sessionIds: string[]
}

export interface SchedEventData {
  timezone: string
  sessions: SchedSession[]
  users: SchedUser[]
  registrations: SchedRegistration[]
  speakers: SchedSpeaker[]
}

type Row = Record<string, unknown>

const USER_FIELDS = 'username,name,email,company,position,location,avatar'
const SPEAKER_FIELDS = 'username,name,sessions'

function text(value: unknown): string | undefined {
  if (typeof value === 'number') {
    return String(value)
  }
  return typeof value === 'string' && value.trim() ? value.trim() : undefined
}

function rows(subdomain: string, path: string, body: unknown): Row[] {
  if (!Array.isArray(body)) {
    throw new SchedCallError(`${subdomain}: ${path} did not return a list`)
  }
  return body.filter((row): row is Row => typeof row === 'object' && row !== null)
}

function toSession(row: Row): SchedSession | null {
  const id = text(row.id)
  const key = text(row.event_key)
  const name = text(row.name)
  const start = text(row.event_start)
  if (!id || !key || !name || !start) {
    return null
  }
  return { id, key, name, start, type: text(row.event_type), venue: text(row.venue) }
}

function toUser(row: Row): SchedUser | null {
  const username = text(row.username)
  if (!username) {
    return null
  }
  return {
    username,
    name: text(row.name),
    email: text(row.email),
    company: text(row.company),
    position: text(row.position),
    location: text(row.location),
    avatar: text(row.avatar),
  }
}

function toRegistration(row: Row): SchedRegistration | null {
  const username = text(row.username)
  const sessionKey = text(row.event_key)
  if (!username || !sessionKey) {
    return null
  }
  return {
    username,
    sessionKey,
    name: text(row.name),
    updated: text(row.updated),
    company: text(row.company),
    position: text(row.position),
  }
}

function toSpeaker(row: Row): SchedSpeaker | null {
  const username = text(row.username)
  if (!username) {
    return null
  }
  const sessionIds = Array.isArray(row.sessions)
    ? row.sessions.map(text).filter((id): id is string => !!id)
    : []
  return { username, name: text(row.name), sessionIds }
}

function present<T>(items: (T | null)[]): T[] {
  return items.filter((item): item is T => item !== null)
}

// site/sync echoes the API key: only the timezone is kept from it.
export async function readEventTimezone(subdomain: string, key: string): Promise<string> {
  const site = await callSched(subdomain, key, 'site/sync')
  const timezone =
    typeof site === 'object' && site !== null ? text((site as Row).timezone) : undefined
  if (!timezone || !moment.tz.zone(timezone)) {
    throw new SchedCallError(`${subdomain}: site/sync has no valid timezone`)
  }
  return timezone
}

export async function readUsers(subdomain: string, key: string): Promise<SchedUser[]> {
  const body = await callSched(subdomain, key, 'user/list', { fields: USER_FIELDS })
  return present(rows(subdomain, 'user/list', body).map(toUser))
}

export async function readEvent(subdomain: string, key: string): Promise<SchedEventData> {
  const timezone = await readEventTimezone(subdomain, key)
  const sessions = present(
    rows(subdomain, 'session/list', await callSched(subdomain, key, 'session/list')).map(toSession),
  )
  const users = await readUsers(subdomain, key)
  const registrations = present(
    rows(subdomain, 'user/sessions', await callSched(subdomain, key, 'user/sessions')).map(
      toRegistration,
    ),
  )
  const speakers = present(
    rows(
      subdomain,
      'role/export',
      await callSched(subdomain, key, 'role/export', { role: 'speaker', fields: SPEAKER_FIELDS }),
    ).map(toSpeaker),
  )
  return { timezone, sessions, users, registrations, speakers }
}
