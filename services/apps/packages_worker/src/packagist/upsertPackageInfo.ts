import {
  getOrCreateRepoByUrl,
  getPackageHomepage,
  logAuditFieldChanges,
  removeDeclaredPackageRepo,
  setPackageRepositoryUrl,
  updatePackagistPackageStats,
  upsertPackageMaintainers,
  upsertPackageRepo,
} from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { canonicalizeRepoUrl } from '../utils/canonicalizeRepoUrl'
import { resolveManifestRepo } from '../utils/resolveManifestRepo'
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
): Promise<{
  found: boolean
  changedFields: string[]
  packageId: string | null
  hasPrimaryRepo: boolean
}> {
  // Registry data can contain NUL bytes (e.g. mojibake descriptions) that Postgres
  // text columns reject; strip them before any field is persisted.
  stripNullBytesDeep(stats)

  // Packagist's repository field is free-form/author-supplied. canonicalizeRepoUrl's
  // 'other' bucket also matches non-repo URLs (wikis, issue trackers, registry pages)
  // that happen to have 2+ path segments, so only trust the verified SCM hosts here —
  // github.com/gitlab.com/bitbucket.org — rather than the shared utility's default,
  // which other callers (npm/maven/cargo) rely on staying permissive.
  const declared = stats.repositoryUrl ? canonicalizeRepoUrl(stats.repositoryUrl) : null
  const primaryRepo = declared && declared.host !== 'other' ? declared : null

  let found = false
  let packageId: string | null = null
  const changedFields: string[] = []

  await qx.tx(async (t) => {
    // The version manifests carry the homepage, not this endpoint — peek at the currently
    // stored homepage so a package that only declares a homepage still gets a link, without
    // a second write once the stats row is updated below.
    const storedHomepage = await getPackageHomepage(t, purl)
    const resolvedRepo = primaryRepo
      ? { repo: primaryRepo, signal: 'primary' as const }
      : resolveManifestRepo([{ field: 'homepage', url: storedHomepage, signal: 'secondary' }])

    // Step 1: Update packages row
    const result = await updatePackagistPackageStats(t, {
      purl,
      description: stats.description,
      declaredRepositoryUrl: stats.repositoryUrl,
      repositoryUrl: resolvedRepo?.repo.url ?? null,
      status: stats.status,
      totalDownloads: stats.downloadsTotal,
      dependentCount: stats.dependents,
    })

    if (!result) return

    found = true
    const { id, isCritical } = result
    packageId = id
    changedFields.push(...result.changedFields)

    // When there's no trusted repo (removed from the manifest, or no longer
    // canonicalizable to a known host), or it now resolves to a different repo, clear any
    // previously-declared link that no longer applies — package_repos' unique key is
    // (package_id, repo_id), not (package_id, source), so upserting the new link alone
    // would leave a stale one dangling.
    if (resolvedRepo) {
      const repo = await getOrCreateRepoByUrl(t, resolvedRepo.repo.url, resolvedRepo.repo.host)
      const linkChanged = await upsertPackageRepo(t, id, repo.id, {
        source: 'declared',
        signal: resolvedRepo.signal,
      })
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

  return { found, changedFields, packageId, hasPrimaryRepo: !!primaryRepo }
}

// Phase 1 (dynamic endpoint) resolves the homepage-fallback repo from whatever homepage
// is already stored, but the p2 endpoint (phase 2) is what actually carries a new/changed
// homepage — see ingestOnePackagistMetadata. Called after phase 2 persists, so a package
// with no declared repository field still gets linked to its homepage in the same run it's
// first seen, instead of waiting for the next scheduled ingestion.
export async function reconcilePackagistHomepageRepo(
  qx: QueryExecutor,
  purl: string,
  packageId: string,
  homepage: string | null,
): Promise<string[]> {
  const resolved = resolveManifestRepo([{ field: 'homepage', url: homepage, signal: 'secondary' }])
  const changedFields: string[] = []

  await qx.tx(async (t) => {
    changedFields.push(...(await setPackageRepositoryUrl(t, packageId, resolved?.repo.url ?? null)))
    if (resolved) {
      const repo = await getOrCreateRepoByUrl(t, resolved.repo.url, resolved.repo.host)
      const linkChanged = await upsertPackageRepo(t, packageId, repo.id, {
        source: 'declared',
        signal: 'secondary',
      })
      const removedFields = await removeDeclaredPackageRepo(t, packageId, repo.id)
      changedFields.push(...repo.changedFields, ...linkChanged, ...removedFields)
    } else {
      const removedFields = await removeDeclaredPackageRepo(t, packageId)
      changedFields.push(...removedFields)
    }
    await logAuditFieldChanges(t, WORKER, purl, changedFields)
  })

  return changedFields
}
