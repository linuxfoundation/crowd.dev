// Accepts a bare npm name or a full purl (see blastRadiusJobRequestSchema) and reduces
// it to the bare form OSV/the npm registry compare against.
export function toBareNpmName(input: string): string {
  let name = input.trim()

  const q = name.indexOf('?')
  const h = name.indexOf('#')
  const cut = q === -1 ? h : h === -1 ? q : Math.min(q, h)
  if (cut !== -1) name = name.slice(0, cut)

  if (name.startsWith('pkg:npm/')) {
    name = name.slice('pkg:npm/'.length)
  }

  name = name.replace(/%40/gi, '@')

  // Strip a trailing @version — never a scope separator, which is always followed by `/`.
  name = name.replace(/@[^/@]+$/, '')

  return name
}

// Same normalization as toBareNpmName, but for Go: purls have no %40-escaped scope
// separator to unescape, and module paths never contain '@' themselves.
export function toBareGoModule(input: string): string {
  let name = input.trim()

  const q = name.indexOf('?')
  const h = name.indexOf('#')
  const cut = q === -1 ? h : h === -1 ? q : Math.min(q, h)
  if (cut !== -1) name = name.slice(0, cut)

  name = decodeURIComponent(name)

  if (name.startsWith('pkg:golang/')) {
    name = name.slice('pkg:golang/'.length)
  }

  name = name.replace(/@[^/@]+$/, '')

  return name
}

// Same normalization as toBareGoModule, but for Cargo: crates.io purls spell the
// ecosystem 'cargo' and crate names never contain '@' themselves either.
export function toBareCargoName(input: string): string {
  let name = input.trim()

  const q = name.indexOf('?')
  const h = name.indexOf('#')
  const cut = q === -1 ? h : h === -1 ? q : Math.min(q, h)
  if (cut !== -1) name = name.slice(0, cut)

  name = decodeURIComponent(name)

  if (name.startsWith('pkg:cargo/')) {
    name = name.slice('pkg:cargo/'.length)
  }

  name = name.replace(/@[^/@]+$/, '')

  return name
}

// packages/purl rows store cargo names '_'-normalized (see cargo/loadDump.ts) while
// OSV/crates.io use '-'. Apply ONLY at the packages-table lookup boundary.
export function toDbCargoName(name: string): string {
  return name.toLowerCase().replace(/-/g, '_')
}

// Maven has no single "bare name" — accepts either the "groupId:artifactId" coordinate
// (OSV's package.name spelling) or a purl (pkg:maven/groupId/artifactId@version).
export function toBareMavenCoordinate(input: string): { groupId: string; artifactId: string } {
  let name = input.trim()

  const q = name.indexOf('?')
  const h = name.indexOf('#')
  const cut = q === -1 ? h : h === -1 ? q : Math.min(q, h)
  if (cut !== -1) name = name.slice(0, cut)

  name = decodeURIComponent(name)

  if (name.startsWith('pkg:maven/')) {
    name = name.slice('pkg:maven/'.length)
    name = name.replace(/@[^/@]+$/, '')
    const slash = name.indexOf('/')
    if (slash === -1) return { groupId: name, artifactId: '' }
    return { groupId: name.slice(0, slash), artifactId: name.slice(slash + 1) }
  }

  name = name.replace(/@[^/@]+$/, '')
  const colon = name.indexOf(':')
  if (colon === -1) return { groupId: '', artifactId: name }
  return { groupId: name.slice(0, colon), artifactId: name.slice(colon + 1) }
}
