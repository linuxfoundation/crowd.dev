import { parseGithubUrl } from '../../enricher/fetchLightRepo'
import { Extractor, ExtractorResult } from '../types'

import { GITHUB_API, fetchJson, githubAuthHeaders } from './http'

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
  let owner: string
  let name: string
  try {
    ;({ owner, name } = parseGithubUrl(target.url))
  } catch {
    return { contacts: [], policies: {} }
  }

  // The endpoint works unauthenticated, but we use the token pool for rate-limit budget.
  const token = deps.getToken ? await deps.getToken() : null
  const headers = token ? githubAuthHeaders(token) : {}
  const { json } = await fetchJson(
    `${GITHUB_API}/repos/${owner}/${name}/private-vulnerability-reporting`,
    deps.fetchTimeoutMs,
    headers,
  )
  if (!json) return { contacts: [], policies: {} }

  return mapPvr(json, owner, name, new Date().toISOString())
}
