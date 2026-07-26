// GitHub patch fetcher — downloads .patch files for commits and PRs.

// Parse a GitHub URL to create a slug for caching.
export function patchSlug(url: string): string | null {
  const match = url.match(/^https:\/\/github\.com\/([^/]+)\/([^/]+)\/(commit|pull)\/([^/?#]+)/)
  if (!match) return null

  const [, owner, repo, type, ref] = match
  const typeShort = type === 'commit' ? 'commit' : 'pr'
  return `${owner}-${repo}-${typeShort}-${ref.slice(0, 12)}`
}

// Fetch a patch from a GitHub commit or PR URL.
export async function fetchPatch(url: string): Promise<string> {
  const patchUrl = `${url}.patch`
  const res = await fetch(patchUrl)

  if (!res.ok) {
    throw new Error(`Failed to fetch patch from ${patchUrl}: ${res.status} ${res.statusText}`)
  }

  return res.text()
}
