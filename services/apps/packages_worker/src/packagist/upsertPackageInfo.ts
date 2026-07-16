import {
  getOrCreateRepoByUrl,
  updatePackagistPackageStats,
  upsertPackageMaintainers,
  upsertPackageRepo,
} from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { canonicalizeRepoUrl } from '../utils/canonicalizeRepoUrl'
import { stripNullBytesDeep } from '../utils/stripNullBytesDeep'

import type { NormalizedPackagistStats } from './types'

// Dynamic-endpoint persistence: packages fields + repo link for ALL packages,
// maintainers only for critical ones. Download rows are NOT written here — they
// belong to the dedicated downloads-30d/daily lanes.
export async function persistPackagistPackageInfo(
  qx: QueryExecutor,
  purl: string,
  stats: NormalizedPackagistStats,
): Promise<{ found: boolean; changedFields: string[] }> {
  // Registry data can contain NUL bytes (e.g. mojibake descriptions) that Postgres
  // text columns reject; strip them before any field is persisted.
  stripNullBytesDeep(stats)

  // Step 1: Update packages row
  const canonical = stats.repositoryUrl ? canonicalizeRepoUrl(stats.repositoryUrl) : null
  const result = await updatePackagistPackageStats(qx, {
    purl,
    description: stats.description,
    declaredRepositoryUrl: stats.repositoryUrl,
    repositoryUrl: canonical?.url ?? null,
    status: stats.status,
    downloadsLast30d: stats.downloadsMonthly,
    totalDownloads: stats.downloadsTotal,
    dependentCount: stats.dependents,
  })

  if (!result) {
    return { found: false, changedFields: [] }
  }

  const { id, isCritical } = result
  const changedFields = [...result.changedFields]

  // Step 2: Link the repo for ALL packages — 'declared'/0.8 is the manifest-declared
  // convention shared by npm/pypi/maven/cargo
  if (canonical) {
    const repo = await getOrCreateRepoByUrl(qx, canonical.url, canonical.host)
    const linkChanged = await upsertPackageRepo(qx, id, repo.id, 'declared', 0.8)
    changedFields.push(...repo.changedFields, ...linkChanged)
  }

  // Step 3: Maintainers only for critical packages
  if (isCritical && stats.maintainers.length > 0) {
    await upsertPackageMaintainers(qx, id, stats.maintainers, 'packagist')
  }

  return { found: true, changedFields }
}
