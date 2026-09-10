import { ParsedProtocol } from './types'

const METHOD_TYPES = new Set([
  'github-pvr',
  'email',
  'web-form',
  'bounty-platform',
  'security-txt',
  'mailing-list',
])
const METHOD_STATUSES = new Set(['preferred', 'accepted', 'fallback', 'prohibited'])
export const SENTINEL_ENDPOINT_BY_TYPE: Record<string, string> = {
  'github-pvr': 'github-pvr',
  'security-txt': 'security.txt',
}

function normalize(text: string): string {
  return text
    .toLowerCase()
    .replace(/\s*(?:\[at\]|\(at\)| at )\s*/g, '@')
    .replace(/\s*(?:\[dot\]|\(dot\)| dot )\s*/g, '.')
    .replace(/\s+/g, ' ')
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((v) => typeof v === 'string')
}

function hasValidGuidelinesShape(parsed: ParsedProtocol): boolean {
  const g = parsed.guidelines
  if (g === null || g === undefined) return true
  return (
    typeof g === 'object' &&
    isStringArray(g.generalPrinciples) &&
    isStringArray(g.avoid) &&
    Array.isArray(g.recommend) &&
    g.recommend.every((r) => r && typeof r.scenario === 'string' && typeof r.action === 'string')
  )
}

export function validateParsedProtocol(
  parsed: ParsedProtocol,
  sourceText: string,
): { ok: boolean; reasons: string[] } {
  if (!parsed || !Array.isArray(parsed.methods)) {
    return { ok: false, reasons: ['methods is not an array'] }
  }

  const reasons: string[] = []
  if (!hasValidGuidelinesShape(parsed)) reasons.push('malformed guidelines')

  const lowerSource = sourceText.toLowerCase()
  const normalizedSource = normalize(sourceText)

  let preferredCount = 0
  for (const m of parsed.methods) {
    if (!m || typeof m !== 'object') {
      reasons.push('malformed method entry')
      continue
    }
    if (!METHOD_TYPES.has(m.type)) reasons.push(`invalid type: ${m.type}`)
    if (!METHOD_STATUSES.has(m.status)) reasons.push(`invalid status: ${m.status}`)
    if (m.condition !== null && m.condition !== undefined && typeof m.condition !== 'string') {
      reasons.push(`malformed condition on: ${m.type}`)
    }
    if (m.status === 'preferred') preferredCount++
    if (typeof m.endpoint !== 'string' || m.endpoint.trim() === '') {
      reasons.push(`empty endpoint on: ${m.type}`)
      continue
    }
    if (SENTINEL_ENDPOINT_BY_TYPE[m.type] === m.endpoint) continue
    const needle = m.endpoint.toLowerCase()
    if (!lowerSource.includes(needle) && !normalizedSource.includes(needle)) {
      reasons.push(`endpoint not in source: ${m.endpoint}`)
    }
  }
  if (preferredCount > 1) reasons.push(`multiple preferred methods: ${preferredCount}`)

  return { ok: reasons.length === 0, reasons }
}
