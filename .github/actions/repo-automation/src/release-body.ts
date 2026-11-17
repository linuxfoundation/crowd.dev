const MARKER_RE = /<!-- release-services: (.*?) -->/

export function renderReleaseBody(i: {
  title: string
  number: number | null
  author: string | null
  services: string[] | null
}): string {
  const head = [
    i.title,
    i.number === null ? null : `(#${i.number})`,
    i.author === null ? null : `— @${i.author}`,
  ]
    .filter((part) => part !== null)
    .join(' ')
  const lines = [head]
  if (i.services === null) lines.push('Services: all (shared code)')
  else if (i.services.length > 0) lines.push(`Services: ${i.services.join(', ')}`)
  lines.push(`<!-- release-services: ${JSON.stringify(i.services ?? '*')} -->`)
  return lines.join('\n')
}

/** Services recorded in a release body; null when the marker is `*`, missing or unreadable. */
export function parseReleaseServices(body: string): string[] | null {
  const m = MARKER_RE.exec(body)
  if (!m) return null
  try {
    const value: unknown = JSON.parse(m[1])
    return Array.isArray(value) ? (value as string[]) : null
  } catch {
    return null
  }
}
