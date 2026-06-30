import { parseProxies, proxyCount, type ProxyEndpoint } from '../proxies'

// Global kill-switch for the npm proxy layer. When off (the default), every npm worker
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
