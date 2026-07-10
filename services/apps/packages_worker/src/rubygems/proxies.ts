import { parseProxies } from '../proxies'

export function rubyGemsProxiesEnabled(): boolean {
  const raw = (process.env.CROWD_PACKAGES_PROXIES_ENABLED ?? '').trim().toLowerCase()
  return raw === 'true' || raw === '1'
}

export function rubyGemsProxyUrls(): string[] {
  if (!rubyGemsProxiesEnabled()) return []
  return parseProxies().map((p) => `http://${p.username}:${p.password}@${p.host}:${p.port}`)
}
