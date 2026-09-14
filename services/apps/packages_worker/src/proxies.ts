// Generic proxy primitives shared across the workers
// The proxy list is shared via CROWD_PACKAGES_PROXIES; each sub-worker owns its own enable
// flag and lane logic (see e.g. npm/proxies.ts, pypi/proxies.ts).

export interface ProxyEndpoint {
  host: string
  port: string
  username: string
  password: string
}

// Parse the shared CROWD_PACKAGES_PROXIES list ("host:port:user:pass,host:port:user:pass").
// Malformed entries (missing any field) are dropped.
export function parseProxies(): ProxyEndpoint[] {
  const raw = process.env.CROWD_PACKAGES_PROXIES
  if (!raw) return []
  return raw
    .split(',')
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0)
    .map((entry) => {
      const [host, port, username, password] = entry.split(':')
      return { host, port, username, password }
    })
    .filter((p) => p.host && p.port && p.username && p.password)
}

export function proxyCount(): number {
  return parseProxies().length
}

export function proxyUrl(p: ProxyEndpoint): string {
  return `http://${p.username}:${p.password}@${p.host}:${p.port}`
}
