import { getServiceChildLogger } from '@crowd/logging'

import { parseGithubUrl } from '../../enricher/fetchLightRepo'
import { ExtractorDeps, ProvenanceEntry, RawContact, RepoTarget } from '../types'

import { isEmail } from './http'

const log = getServiceChildLogger('security-contacts:repo-owner')

const SOURCE = 'github-repo-owner'

export function buildOwnerHandleContact(owner: string, fetchedAt: string, url: string): RawContact {
  return {
    channel: 'github-handle',
    value: owner,
    handle: owner,
    role: 'org-owner',
    tier: 'D',
    provenance: [{ source: SOURCE, sourceTier: 'D', path: url, fetchedAt }],
  }
}

export async function fetchRepoOwner(
  target: RepoTarget,
  deps: Pick<ExtractorDeps, 'githubGet'>,
): Promise<RawContact[]> {
  let owner: string
  try {
    ;({ owner } = parseGithubUrl(target.url))
  } catch {
    return []
  }

  const fetchedAt = new Date().toISOString()
  const contacts: RawContact[] = [buildOwnerHandleContact(owner, fetchedAt, target.url)]

  try {
    const { text } = await deps.githubGet(`/users/${owner}`)
    const email = (text ? (JSON.parse(text) as { email?: unknown }) : null)?.email
    if (typeof email === 'string' && isEmail(email)) {
      const provenance: ProvenanceEntry[] = [
        {
          source: SOURCE,
          sourceTier: 'D',
          path: `https://api.github.com/users/${owner}`,
          fetchedAt,
        },
      ]
      contacts.push({
        channel: 'email',
        value: email,
        handle: owner,
        role: 'org-owner',
        tier: 'D',
        provenance,
      })
    }
  } catch (err) {
    log.warn(
      { repoId: target.repoId, owner, errMsg: (err as Error).message },
      'Owner email lookup failed',
    )
  }

  return contacts
}
