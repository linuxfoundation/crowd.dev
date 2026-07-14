import { isSvnHost } from './extract'

export function isPrerelease(version: string): boolean {
  return /-(SNAPSHOT|alpha|beta|rc|cr|m\d+|dev)/i.test(version)
}

export function parseRepoUrl(
  url: string,
): { host: string; owner: string | null; name: string | null } | null {
  try {
    const parsed = new URL(url)
    const h = parsed.hostname.toLowerCase()
    const parts = parsed.pathname.split('/').filter(Boolean)

    // SVN has no owner/repo concept — its canonical path is a single project
    // identifier (e.g. /repos/asf/geronimo/server), so splitting it into
    // parts[0]/parts[1] like a git host would produce garbage (owner='repos',
    // name='asf'). Keep the whole path as `name` instead.
    if (isSvnHost(h)) return { host: 'svn', owner: null, name: parts.join('/') || null }

    let host: string
    if (h === 'github.com' || h.endsWith('.github.com')) host = 'github'
    else if (h === 'gitlab.com' || h.includes('gitlab')) host = 'gitlab'
    else if (h === 'bitbucket.org') host = 'bitbucket'
    else host = 'other'
    return { host, owner: parts[0] ?? null, name: parts[1] ?? null }
  } catch {
    return null
  }
}
