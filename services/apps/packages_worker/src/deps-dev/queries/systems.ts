export const DEFAULT_SYSTEMS = `'NPM', 'GO', 'MAVEN', 'PYPI', 'NUGET', 'CARGO', 'RUBYGEMS'`

const VALID_SYSTEMS = new Set(['NPM', 'GO', 'MAVEN', 'PYPI', 'NUGET', 'CARGO', 'RUBYGEMS'])

export function toSystemsFilter(ecosystems?: string[]): string {
  if (!ecosystems || ecosystems.length === 0) return DEFAULT_SYSTEMS
  const upper = ecosystems.map((e) => e.toUpperCase())
  for (const e of upper) {
    if (!VALID_SYSTEMS.has(e)) throw new Error(`Invalid ecosystem: ${e}`)
  }
  return upper.map((e) => `'${e}'`).join(', ')
}
