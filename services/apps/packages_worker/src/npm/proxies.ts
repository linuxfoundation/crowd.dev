export interface ProxyEndpoint {
  host: string
  port: string
  username: string
  password: string
}

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

// Global kill-switch for the proxy layer. When off (the default), every npm worker
// runs a single direct lane (no ProxyAgent) — see laneCount/proxyForLane.
export function proxiesEnabled(): boolean {
  const raw = (process.env.CROWD_PACKAGES_PROXIES_ENABLED ?? '').trim().toLowerCase()
  return raw === 'true' || raw === '1'
}

// Number of concurrent lanes shared by all npm workers: one per configured proxy IP
// when enabled (min 1), otherwise a single direct lane.
export function laneCount(): number {
  return proxiesEnabled() ? Math.max(1, proxyCount()) : 1
}

// The proxy endpoint a given lane should egress through, or undefined for a direct
// lane (proxies disabled, or no proxy configured at this index).
export function proxyForLane(laneIndex: number): ProxyEndpoint | undefined {
  if (!proxiesEnabled()) return undefined
  return parseProxies()[laneIndex]
}
