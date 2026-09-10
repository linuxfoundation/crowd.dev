import { OPENDALIGHT_GERRIT_GITWEB_PATH, OPENDALIGHT_GERRIT_HOST, isSvnHost } from './extract'

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

    // OpenDaylight's canonical link keeps the actual repo identity only in the
    // ?p= query — the pathname is always the fixed /gerrit/gitweb script, so a
    // generic parts[0]/parts[1] split would store owner='gerrit', name='gitweb'
    // for every single OpenDaylight repo, indistinguishable from one another.
    if (h === OPENDALIGHT_GERRIT_HOST && parsed.pathname === OPENDALIGHT_GERRIT_GITWEB_PATH) {
      const p = parsed.searchParams.get('p')
      const name = p ? p.replace(/\.git$/, '') : null
      return { host: 'gerrit', owner: null, name }
    }

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
