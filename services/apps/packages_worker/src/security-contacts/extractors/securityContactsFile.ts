import { getServiceChildLogger } from '@crowd/logging'

import { parseGithubUrl } from '../../enricher/fetchLightRepo'
import { Extractor, ProvenanceEntry, RawContact } from '../types'

import { GITHUB_API, RAW_BASE, fetchJson, fetchText, githubAuthHeaders, isEmail } from './http'

const log = getServiceChildLogger('security-contacts:security_contacts-file')

const SOURCE = 'security_contacts'
const PATH = 'SECURITY_CONTACTS'
const HANDLE_RE = /^[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?$/

export interface SecurityContactEntry {
  handle: string
  email?: string
}

export function parseSecurityContacts(text: string): SecurityContactEntry[] {
  const entries: SecurityContactEntry[] = []
  for (const rawLine of text.split('\n')) {
    const line = rawLine.trim()
    if (!line || line.startsWith('#')) continue

    const tokens = line.replace(/^-\s*/, '').split(/\s+/)
    const handle = tokens[0].replace(/^@/, '')
    if (!HANDLE_RE.test(handle)) continue

    const email = tokens.slice(1).find((t) => isEmail(t))
    entries.push(email ? { handle, email } : { handle })
  }
  return entries
}

async function resolvePublicEmail(
  login: string,
  timeoutMs: number,
  token?: string,
): Promise<string | null> {
  try {
    const { json } = await fetchJson(
      `${GITHUB_API}/users/${login}`,
      timeoutMs,
      token ? githubAuthHeaders(token) : {},
    )
    const email = (json as { email?: unknown } | null)?.email
    return typeof email === 'string' && isEmail(email) ? email : null
  } catch (err) {
    log.warn({ login, errMsg: (err as Error).message }, 'Handle email resolution failed')
    return null
  }
}

export const extractSecurityContactsFile: Extractor = async (target, deps) => {
  let owner: string
  let name: string
  try {
    ;({ owner, name } = parseGithubUrl(target.url))
  } catch {
    return { contacts: [], policies: {} }
  }

  const { text } = await fetchText(`${RAW_BASE}/${owner}/${name}/HEAD/${PATH}`, deps.fetchTimeoutMs)
  if (!text) return { contacts: [], policies: {} }

  const fetchedAt = new Date().toISOString()
  const prov = (): ProvenanceEntry[] => [{ source: SOURCE, sourceTier: 'A', path: PATH, fetchedAt }]

  const token = deps.getToken ? await deps.getToken() : undefined
  const contacts: RawContact[] = []

  for (const entry of parseSecurityContacts(text)) {
    const email =
      entry.email ?? (await resolvePublicEmail(entry.handle, deps.fetchTimeoutMs, token))
    if (email) {
      // name = handle so the reconciler can identity-link this to a bare handle from another source
      contacts.push({
        channel: 'email',
        value: email,
        name: entry.handle,
        role: 'security-team',
        tier: 'A',
        provenance: prov(),
      })
    } else {
      contacts.push({
        channel: 'github-handle',
        value: entry.handle,
        role: 'security-team',
        tier: 'A',
        provenance: prov(),
      })
    }
  }

  return { contacts, policies: {} }
}
