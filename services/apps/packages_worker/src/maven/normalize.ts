export function isPrerelease(version: string): boolean {
  return /-(SNAPSHOT|alpha|beta|rc|cr|m\d+|dev)/i.test(version)
}

export function parseRepoUrl(
  url: string,
): { host: string; owner: string | null; name: string | null } | null {
  try {
    const parsed = new URL(url)
    const h = parsed.hostname.toLowerCase()
    let host: string
    if (h === 'github.com' || h.endsWith('.github.com')) host = 'github'
    else if (h === 'gitlab.com' || h.includes('gitlab')) host = 'gitlab'
    else if (h === 'bitbucket.org') host = 'bitbucket'
    else host = 'other'
    const parts = parsed.pathname.split('/').filter(Boolean)
    return { host, owner: parts[0] ?? null, name: parts[1] ?? null }
  } catch {
    return null
  }
}
