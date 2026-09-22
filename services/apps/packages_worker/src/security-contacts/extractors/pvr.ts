import { parseGithubUrl } from '../../enricher/fetchLightRepo'
import { Extractor, ExtractorResult } from '../types'

const SOURCE = 'pvr'

/* eslint-disable @typescript-eslint/no-explicit-any */

export function mapPvr(
  body: unknown,
  owner: string,
  name: string,
  fetchedAt: string,
): ExtractorResult {
  const enabled = (body as any)?.enabled

  if (typeof enabled !== 'boolean') return { contacts: [], policies: {} }
  if (!enabled) return { contacts: [], policies: { pvrEnabled: false } }

  const url = `https://github.com/${owner}/${name}/security/advisories/new`
  return {
    contacts: [
      {
        channel: 'github-pvr',
        value: url,
        role: 'security-team',
        tier: 'A',
        provenance: [{ source: SOURCE, sourceTier: 'A', fetchedAt }],
      },
    ],
    policies: { pvrEnabled: true, vulnerabilityReportingUrl: url },
  }
}

export const extractPvr: Extractor = async (target, deps) => {
  // GitHub's PVR endpoint rejects archived (and private) repos with 422; skip the call for
  // known-archived repos. Non-archived-yet-unknown repos still get the 422→unknown safety net.
  if (target.archived) return { contacts: [], policies: {} }

  let owner: string
  let name: string
  try {
    ;({ owner, name } = parseGithubUrl(target.url))
  } catch {
    return { contacts: [], policies: {} }
  }

  const { text } = await deps.githubGet(`/repos/${owner}/${name}/private-vulnerability-reporting`)
  if (!text) return { contacts: [], policies: {} }

  return mapPvr(JSON.parse(text), owner, name, new Date().toISOString())
}
