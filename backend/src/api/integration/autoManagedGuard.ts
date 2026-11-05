import SequelizeRepository from '@/database/repositories/sequelizeRepository'
import { Error400 } from '@crowd/common'
import { findIntegrationPlatformsByIds } from '@crowd/data-access-layer/src/integrations'
import { AUTO_MANAGED_INTEGRATION_PLATFORMS } from '@crowd/types'

export function isAutoManagedPlatform(platform: unknown): boolean {
  return AUTO_MANAGED_INTEGRATION_PLATFORMS.some((p) => p === platform)
}

export function assertNoAutoManagedPlatform(language: string, platforms: unknown[]): void {
  if (platforms.some(isAutoManagedPlatform)) {
    throw new Error400(language, 'errors.integrations.autoManaged')
  }
}

// Postgres also accepts uuids without hyphens or wrapped in braces, so normalize before the lookup.
export function toCanonicalUuid(id: unknown): string | null {
  if (typeof id !== 'string') {
    return null
  }
  const hex = id
    .trim()
    .replace(/^\{(.*)\}$/, '$1')
    .replace(/-/g, '')
    .toLowerCase()
  if (!/^[0-9a-f]{32}$/.test(hex)) {
    return null
  }
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20),
  ].join('-')
}

// Query parsing can nest ids in arrays or objects (e.g. ids[0][0]=…), so collect every leaf.
export function collectIdLeaves(ids: unknown): unknown[] {
  if (Array.isArray(ids)) {
    return ids.flatMap(collectIdLeaves)
  }
  if (ids !== null && typeof ids === 'object') {
    return Object.values(ids).flatMap(collectIdLeaves)
  }
  return [ids]
}

// Malformed or unknown ids are skipped, so requests for other platforms fail exactly as before.
export async function assertNoAutoManagedIntegration(req, ids: unknown): Promise<void> {
  const uuids = collectIdLeaves(ids)
    .map(toCanonicalUuid)
    .filter((id): id is string => id !== null)
  const platforms = await findIntegrationPlatformsByIds(
    SequelizeRepository.getQueryExecutor(req),
    uuids,
  )
  assertNoAutoManagedPlatform(req.language, platforms)
}
