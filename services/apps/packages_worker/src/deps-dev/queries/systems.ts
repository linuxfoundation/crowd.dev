export const DEFAULT_SYSTEMS = `'NPM', 'GO', 'MAVEN', 'PYPI', 'NUGET', 'CARGO'`

export function toSystemsFilter(ecosystems?: string[]): string {
  if (!ecosystems || ecosystems.length === 0) return DEFAULT_SYSTEMS
  return ecosystems.map((e) => `'${e.toUpperCase()}'`).join(', ')
}
