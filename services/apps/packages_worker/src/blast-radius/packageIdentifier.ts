function stripQueryAndFragment(input: string): string {
  const q = input.indexOf('?')
  const h = input.indexOf('#')
  const cut = q === -1 ? h : h === -1 ? q : Math.min(q, h)
  return cut === -1 ? input : input.slice(0, cut)
}

// OSV/npm registry use bare names only, so purls must be normalized before comparison.
// See blastRadiusJobRequestSchema for accepted formats.
export function toBareNpmName(input: string): string {
  let name = input.trim()

  name = stripQueryAndFragment(name)

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

  name = stripQueryAndFragment(name)

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

  name = stripQueryAndFragment(name)

  name = decodeURIComponent(name)

  if (name.startsWith('pkg:cargo/')) {
    name = name.slice('pkg:cargo/'.length)
  }

  name = name.replace(/@[^/@]+$/, '')

  return name
}

// Same normalization as toBareGoModule, but for NuGet. Deliberately does NOT lowercase —
// findPackageId/findPackageIdsByName compare case-sensitively against canonical casing.
export function toBareNuGetId(input: string): string {
  let name = input.trim()

  name = stripQueryAndFragment(name)

  name = decodeURIComponent(name)

  if (name.startsWith('pkg:nuget/')) {
    name = name.slice('pkg:nuget/'.length)
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

  name = stripQueryAndFragment(name)

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
