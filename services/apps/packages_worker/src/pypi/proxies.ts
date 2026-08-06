import { type ProxyEndpoint, parseProxies } from '../proxies'

// Off by default: when disabled, the single PyPI lane egresses directly (no ProxyAgent).
// The proxy list is shared with other workers via CROWD_PACKAGES_PROXIES; only the enable
// flag (CROWD_PACKAGES_PYPI_PROXIES_ENABLED) is PyPI-specific.
export function pypiProxiesEnabled(): boolean {
  const raw = (process.env.CROWD_PACKAGES_PYPI_PROXIES_ENABLED ?? '').trim().toLowerCase()
  return raw === 'true' || raw === '1'
}

// The proxy endpoints the PyPI lane may rotate through, or [] when disabled/unconfigured.
export function pypiProxyPool(): ProxyEndpoint[] {
  return pypiProxiesEnabled() ? parseProxies() : []
}
