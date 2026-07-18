import {
  getOrCreateRepoByUrl,
  logAuditFieldChanges,
  removeDeclaredPackageRepo,
  updatePackagistPackageStats,
  upsertPackageMaintainers,
  upsertPackageRepo,
} from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { canonicalizeRepoUrl } from '../utils/canonicalizeRepoUrl'
import { stripNullBytesDeep } from '../utils/stripNullBytesDeep'

import type { NormalizedPackagistStats } from './types'

const WORKER = 'packagist'

// Dynamic-endpoint persistence: packages fields + repo link for ALL packages,
// maintainers only for critical ones. Download rows are NOT written here — they
// belong to the dedicated downloads-30d/daily lanes. All writes AND the audit
// record share one transaction so a failure partway through — including a
// failed audit insert — can never leave the writes and their audit trail
// inconsistent with each other, and a retry can't lose an already-committed
// change's audit event.
export async function persistPackagistPackageInfo(
  qx: QueryExecutor,
  purl: string,
  stats: NormalizedPackagistStats,
): Promise<{ found: boolean; changedFields: string[] }> {
  // Registry data can contain NUL bytes (e.g. mojibake descriptions) that Postgres
  // text columns reject; strip them before any field is persisted.
  stripNullBytesDeep(stats)

  const canonical = stats.repositoryUrl ? canonicalizeRepoUrl(stats.repositoryUrl) : null
  // Packagist's repository field is free-form/author-supplied. canonicalizeRepoUrl's
  // 'other' bucket also matches non-repo URLs (wikis, issue trackers, registry pages)
  // that happen to have 2+ path segments, so only trust the verified SCM hosts here —
  // github.com/gitlab.com/bitbucket.org — rather than the shared utility's default,
  // which other callers (npm/maven/cargo) rely on staying permissive.
  const trustedRepo = canonical && canonical.host !== 'other' ? canonical : null

  let found = false
  const changedFields: string[] = []

  await qx.tx(async (t) => {
    // Step 1: Update packages row
    const result = await updatePackagistPackageStats(t, {
      purl,
      description: stats.description,
      declaredRepositoryUrl: stats.repositoryUrl,
      repositoryUrl: trustedRepo?.url ?? null,
      status: stats.status,
      totalDownloads: stats.downloadsTotal,
      dependentCount: stats.dependents,
    })

    if (!result) return

    found = true
    const { id, isCritical } = result
    changedFields.push(...result.changedFields)

    // Step 2: Link the repo for ALL packages — 'declared'/0.8 is the manifest-declared
    // convention shared by npm/pypi/maven/cargo. When there's no trusted repo (removed
    // from the manifest, or no longer canonicalizable to a known host), or it now
    // resolves to a different repo, clear any previously-declared link that no longer
    // applies — package_repos' unique key is (package_id, repo_id), not (package_id,
    // source), so upserting the new link alone would leave a stale one dangling.
    if (trustedRepo) {
      const repo = await getOrCreateRepoByUrl(t, trustedRepo.url, trustedRepo.host)
      const linkChanged = await upsertPackageRepo(t, id, repo.id, 'declared', 0.8)
      const removedFields = await removeDeclaredPackageRepo(t, id, repo.id)
      changedFields.push(...repo.changedFields, ...linkChanged, ...removedFields)
    } else {
      const removedFields = await removeDeclaredPackageRepo(t, id)
      changedFields.push(...removedFields)
    }

    // Step 3: Maintainers only for critical packages. upsertPackageMaintainers always
    // replaces the full stored set (including deleting rows for maintainers no longer
    // reported), so it must run even when the registry now reports zero maintainers —
    // skipping it on an empty list would leave stale maintainers attached forever.
    if (isCritical) {
      const maintainerChanges = await upsertPackageMaintainers(
        t,
        id,
        stats.maintainers,
        'packagist',
      )
      changedFields.push(...maintainerChanges)
    }

    await logAuditFieldChanges(t, WORKER, purl, changedFields)
  })

  return { found, changedFields }
}
