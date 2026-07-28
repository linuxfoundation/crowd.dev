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
const SENTINEL_ENDPOINTS = new Set(['github-pvr', 'security.txt'])

function normalize(text: string): string {
  return text
    .toLowerCase()
    .replace(/\s*(?:\[at\]|\(at\)| at )\s*/g, '@')
    .replace(/\s*(?:\[dot\]|\(dot\)| dot )\s*/g, '.')
    .replace(/\s+/g, ' ')
}

export function validateParsedProtocol(
  parsed: ParsedProtocol,
  sourceText: string,
): { ok: boolean; reasons: string[] } {
  const reasons: string[] = []
  const lowerSource = sourceText.toLowerCase()
  const normalizedSource = normalize(sourceText)

  let preferredCount = 0
  for (const m of parsed.methods) {
    if (!METHOD_TYPES.has(m.type)) reasons.push(`invalid type: ${m.type}`)
    if (!METHOD_STATUSES.has(m.status)) reasons.push(`invalid status: ${m.status}`)
    if (m.status === 'preferred') preferredCount++
    if (SENTINEL_ENDPOINTS.has(m.endpoint)) continue
    const needle = m.endpoint.toLowerCase()
    if (!lowerSource.includes(needle) && !normalizedSource.includes(needle)) {
      reasons.push(`endpoint not in source: ${m.endpoint}`)
    }
  }
  if (preferredCount > 1) reasons.push(`multiple preferred methods: ${preferredCount}`)

  return { ok: reasons.length === 0, reasons }
}
