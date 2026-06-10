const ECOSYSTEM_PREFIXES: Array<[string, string]> = [
  ['pkg:npm', 'npm'],
  ['pkg:maven', 'maven'],
]

export function extractEcosystem(purl: string): string {
  for (const [prefix, ecosystem] of ECOSYSTEM_PREFIXES) {
    if (purl.startsWith(prefix)) return ecosystem
  }
  return 'unknown'
}

export function extractName(purl: string): string {
  const lastSegment = purl.split('/').pop() ?? purl
  const withoutVersion = lastSegment.split('@')[0]
  return withoutVersion || purl
}
