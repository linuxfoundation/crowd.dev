import {
  getOrCreateRepoByUrl,
  removeDeclaredPackageRepo,
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
  // Packagist's repository field is free-form/author-supplied. canonicalizeRepoUrl's
  // 'other' bucket also matches non-repo URLs (wikis, issue trackers, registry pages)
  // that happen to have 2+ path segments, so only trust the verified SCM hosts here —
  // github.com/gitlab.com/bitbucket.org — rather than the shared utility's default,
  // which other callers (npm/maven/cargo) rely on staying permissive.
  const trustedRepo = canonical && canonical.host !== 'other' ? canonical : null
  const result = await updatePackagistPackageStats(qx, {
    purl,
    description: stats.description,
    declaredRepositoryUrl: stats.repositoryUrl,
    repositoryUrl: trustedRepo?.url ?? null,
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
  // convention shared by npm/pypi/maven/cargo. When there's no trusted repo (removed
  // from the manifest, or no longer canonicalizable to a known host), clear any
  // previously-declared link instead of leaving it pointing at a repo the package no
  // longer declares.
  if (trustedRepo) {
    const repo = await getOrCreateRepoByUrl(qx, trustedRepo.url, trustedRepo.host)
    const linkChanged = await upsertPackageRepo(qx, id, repo.id, 'declared', 0.8)
    changedFields.push(...repo.changedFields, ...linkChanged)
  } else {
    const removedFields = await removeDeclaredPackageRepo(qx, id)
    changedFields.push(...removedFields)
  }

  // Step 3: Maintainers only for critical packages
  if (isCritical && stats.maintainers.length > 0) {
    const maintainerChanges = await upsertPackageMaintainers(qx, id, stats.maintainers, 'packagist')
    changedFields.push(...maintainerChanges)
  }

  return { found: true, changedFields }
}
