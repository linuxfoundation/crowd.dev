export interface ParsedPurl {
  type: string
  namespace: string | null
  name: string
}

/**
 * Minimal Package URL parser: pkg:TYPE/NAMESPACE/NAME@VERSION?QUALIFIERS#SUBPATH.
 * Only type/namespace/name are needed to address a registry.
 */
export function parsePurl(purl: string): ParsedPurl | null {
  if (!purl || !purl.startsWith('pkg:')) return null

  let rest = purl.slice('pkg:'.length)
  rest = rest.split('#')[0].split('?')[0]

  const slash = rest.indexOf('/')
  if (slash === -1) return null

  const type = rest.slice(0, slash).toLowerCase()
  let path = rest.slice(slash + 1)

  // Version is the last @ segment (but not a leading @scope on the name).
  const at = path.lastIndexOf('@')
  if (at > 0) path = path.slice(0, at)

  const segments = path.split('/').filter(Boolean).map(decodeURIComponent)
  if (segments.length === 0) return null

  const name = segments[segments.length - 1]
  const namespace = segments.length > 1 ? segments.slice(0, -1).join('/') : null
  return { type, namespace, name }
}
