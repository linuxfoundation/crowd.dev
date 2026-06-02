const HOST_BY_TYPE: Record<string, string> = {
  GITHUB: 'github.com',
  GITLAB: 'gitlab.com',
  BITBUCKET: 'bitbucket.org',
}

export function canonicalRepoUrl(type: string, name: string): string | null {
  const host = HOST_BY_TYPE[type]
  if (!host) return null
  // Names are bare paths (owner/repo or owner/subgroup/repo) — no host prefix.
  // Strip full URL prefix only if present; bare paths pass through untouched.
  // TODO(Step 2a): verify against sampled ProjectName shapes from BQ console.
  const stripped = name
    .trim()
    .replace(/^https?:\/\/[^/]+\//, '')
    .replace(/\.git$/, '')
    .replace(/\/$/, '')
  // GitHub enforces case-insensitive uniqueness (can't create Foo and foo as separate orgs/repos),
  // so lowercasing is always safe and aligns with deps.dev's canonical form.
  // GitLab, Bitbucket, and self-hosted forges are case-sensitive — preserve original casing.
  const path = host === 'github.com' ? stripped.toLowerCase() : stripped
  if (!/^[a-zA-Z0-9._-]+\/[a-zA-Z0-9._/-]+$/.test(path)) return null
  return `https://${host}/${path}`
}

export function parseRepoUrl(url: string): { host: string; owner: string; name: string } {
  const u = new URL(url)
  const parts = u.pathname.slice(1).split('/')
  return {
    host: u.hostname,
    owner: parts[0],
    name: parts.slice(1).join('/'),
  }
}
