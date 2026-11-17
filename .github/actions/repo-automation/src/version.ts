export type Bump = 'major' | 'minor' | 'patch'

const TITLE_RE = /^(\w+)(\([^)]*\))?(!)?: (.+)$/

/** Parses a Conventional Commits title; null when the title does not follow the format. */
export function parseTitle(
  title: string,
): { type: string; breaking: boolean; description: string } | null {
  const m = TITLE_RE.exec(title)
  if (!m) return null
  return { type: m[1], breaking: m[3] === '!', description: m[4] }
}

/** Bump level for a PR title: breaking is major, feat is minor, everything else is patch. */
export function bumpFor(title: string): Bump {
  const parsed = parseTitle(title)
  if (!parsed) return 'patch'
  if (parsed.breaking) return 'major'
  return parsed.type === 'feat' ? 'minor' : 'patch'
}

/** Only feat, fix and perf merges produce a GitHub Release. */
export function isReleaseType(title: string): boolean {
  const parsed = parseTitle(title)
  return parsed !== null && ['feat', 'fix', 'perf'].includes(parsed.type)
}

/** Next `vX.Y.Z` tag. A major bump is held to minor while the major version is 0. */
export function nextVersion(latest: string | null, bump: Bump, initial: string): string {
  if (latest === null) return initial
  const m = /^v(\d+)\.(\d+)\.(\d+)$/.exec(latest)
  if (!m) throw new Error(`Malformed latest tag "${latest}", expected vX.Y.Z`)
  const [major, minor, patch] = [Number(m[1]), Number(m[2]), Number(m[3])]
  const level = bump === 'major' && major === 0 ? 'minor' : bump
  if (level === 'major') return `v${major + 1}.0.0`
  if (level === 'minor') return `v${major}.${minor + 1}.0`
  return `v${major}.${minor}.${patch + 1}`
}

const SEMVER_TAG_RE = /^v(\d+)\.(\d+)\.(\d+)$/

export function isSemverTag(tag: string): boolean {
  return SEMVER_TAG_RE.test(tag)
}

/** Negative, zero or positive like a sort comparator; both arguments must be strict vX.Y.Z tags. */
export function compareVersions(a: string, b: string): number {
  const parse = (tag: string): number[] => {
    const m = SEMVER_TAG_RE.exec(tag)
    if (!m) throw new Error(`Malformed tag "${tag}", expected vX.Y.Z`)
    return [Number(m[1]), Number(m[2]), Number(m[3])]
  }
  const [pa, pb] = [parse(a), parse(b)]
  for (let i = 0; i < 3; i++) if (pa[i] !== pb[i]) return pa[i] - pb[i]
  return 0
}
