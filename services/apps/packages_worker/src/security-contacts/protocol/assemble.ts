import { parseGithubUrl } from '../../enricher/fetchLightRepo'

import {
  AssembledProtocol,
  ParseRowStatus,
  ParsedProtocol,
  ProtocolMethod,
  ProtocolMethodType,
} from './types'
import { SENTINEL_ENDPOINT_BY_TYPE } from './validate'

export interface AssembleInput {
  repoUrl: string
  pvrEnabled: boolean | null
  securityTxtUrl: string | null
  fileParses: Array<{
    blobOid: string
    path: string
    parser: 'deterministic' | 'llm'
    status: ParseRowStatus
    parsed: ParsedProtocol
  }>
  pageParses: Array<{
    hash: string
    url: string
    parser: 'deterministic' | 'llm'
    status: ParseRowStatus
    parsed: ParsedProtocol
  }>
  fallbackContacts: Array<{ channel: string; value: string; score: number }>
}

const STATUS_ORDER: Record<string, number> = {
  preferred: 0,
  accepted: 1,
  fallback: 2,
  prohibited: 3,
}
const FALLBACK_CHANNEL_MAP: Record<string, ProtocolMethodType> = {
  email: 'email',
  'github-pvr': 'github-pvr',
  'web-form': 'web-form',
}

function pvrAdvisoryUrl(repoUrl: string): string {
  const { owner, name } = parseGithubUrl(repoUrl)
  return `https://github.com/${owner}/${name}/security/advisories/new`
}

export function assembleProtocol(input: AssembleInput): AssembledProtocol {
  const methods: ProtocolMethod[] = []
  const seen = new Set<string>()
  const sources: Array<Record<string, string>> = []

  const push = (m: ProtocolMethod) => {
    const key = `${m.type}:${m.endpoint.toLowerCase()}`
    if (seen.has(key)) return
    seen.add(key)
    methods.push(m)
  }

  // Rewrite sentinel endpoints to actionable URLs. github-pvr always resolves
  // from the repo URL; security-txt resolves from repos.security_txt_url when
  // known — a bare "security.txt" mention with no discoverable URL is dropped
  // rather than emitted as a non-actionable endpoint.
  const resolveEndpoint = (m: { type: ProtocolMethodType; endpoint: string }): string | null => {
    if (m.type === 'github-pvr') return pvrAdvisoryUrl(input.repoUrl)
    if (m.type === 'security-txt' && m.endpoint === SENTINEL_ENDPOINT_BY_TYPE['security-txt']) {
      return input.securityTxtUrl
    }
    return m.endpoint
  }

  for (const fp of input.fileParses) {
    if (fp.status === 'degraded') continue
    sources.push({ path: fp.path, blobOid: fp.blobOid, parser: fp.parser })
    for (const m of fp.parsed.methods) {
      const endpoint = resolveEndpoint(m)
      if (endpoint === null) continue
      push({
        ...m,
        endpoint,
        confidence: 'declared',
        provenance: { path: fp.path, blobOid: fp.blobOid, parser: fp.parser },
      })
    }
  }
  for (const pp of input.pageParses) {
    if (pp.status === 'degraded') continue
    sources.push({ url: pp.url, blobOid: pp.hash, parser: pp.parser })
    for (const m of pp.parsed.methods) {
      const endpoint = resolveEndpoint(m)
      if (endpoint === null) continue
      push({
        ...m,
        endpoint,
        confidence: 'declared',
        provenance: { url: pp.url, blobOid: pp.hash, parser: pp.parser },
      })
    }
  }

  let declaredMethods = methods
  if (input.pvrEnabled === false) {
    declaredMethods = declaredMethods.filter((m) => m.type !== 'github-pvr')
  }
  if (input.pvrEnabled === true && !declaredMethods.some((m) => m.type === 'github-pvr')) {
    const hasPreferred = declaredMethods.some((m) => m.status === 'preferred')
    declaredMethods.push({
      type: 'github-pvr',
      status: hasPreferred ? 'accepted' : 'preferred',
      endpoint: pvrAdvisoryUrl(input.repoUrl),
      condition: null,
      confidence: 'declared',
      provenance: { api: 'pvr-flag' },
    })
    sources.push({ api: 'pvr-flag' })
  }
  if (input.securityTxtUrl && !declaredMethods.some((m) => m.type === 'security-txt')) {
    declaredMethods.push({
      type: 'security-txt',
      status: 'accepted',
      endpoint: input.securityTxtUrl,
      condition: null,
      confidence: 'declared',
      provenance: { api: 'security-txt-url' },
    })
    sources.push({ api: 'security-txt-url' })
  }

  const declared = declaredMethods.length > 0

  // security_contacts are always merged in as inferred fallback methods, not
  // just when nothing is declared — the record is the full list of ways to
  // reach out. Dedupe against declared endpoints so a contact that already
  // appears as a declared method is not repeated at lower confidence.
  const declaredKeys = new Set(declaredMethods.map((m) => `${m.type}:${m.endpoint.toLowerCase()}`))
  const inferredMethods: ProtocolMethod[] = []
  for (const c of input.fallbackContacts
    .filter((c) => FALLBACK_CHANNEL_MAP[c.channel])
    .sort((a, b) => b.score - a.score)
    .slice(0, 5)) {
    const type = FALLBACK_CHANNEL_MAP[c.channel]
    const key = `${type}:${c.value.toLowerCase()}`
    if (declaredKeys.has(key)) continue
    declaredKeys.add(key)
    inferredMethods.push({
      type,
      status: 'fallback',
      endpoint: c.value,
      condition: null,
      confidence: 'inferred',
      provenance: { channel: c.channel },
    })
  }
  if (inferredMethods.length > 0) sources.push({ table: 'security_contacts' })

  let finalMethods = [...declaredMethods, ...inferredMethods]

  let preferredSeen = false
  for (const m of finalMethods) {
    if (m.status === 'preferred') {
      if (preferredSeen) m.status = 'accepted'
      preferredSeen = true
    }
  }
  finalMethods = finalMethods
    .map((m, i) => ({ m, i }))
    .sort((a, b) => STATUS_ORDER[a.m.status] - STATUS_ORDER[b.m.status] || a.i - b.i)
    .map((x) => x.m)

  const guidelines =
    [...input.fileParses, ...input.pageParses].find(
      (p) => p.status !== 'degraded' && p.parsed.guidelines,
    )?.parsed.guidelines ?? null

  return { declared, methods: finalMethods, guidelines, sources }
}
