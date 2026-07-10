import {
  QueryExecutor,
  RubyGemsPackageForDependents,
  listRubyGemsPackagesForDependents,
  updateRubyGemsDependentCount,
} from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'
import { type Dispatcher, ProxyAgent } from 'undici'

import { fetchReverseDependencies } from './client'
import { rubyGemsProxyUrls } from './proxies'
import { isRubyGemsFetchError } from './types'

const log = getServiceChildLogger('rubygems-dependents')

export type RubyGemsDependentsConfig = {
  batchSize: number
  concurrency: number
}

export type DependentsBatchResult = {
  processed: number
  notFound: number
  error: number
}

type PackageStatus = 'processed' | 'notFound' | 'error'

async function processPackage(
  qx: QueryExecutor,
  pkg: RubyGemsPackageForDependents,
  dispatcher?: Dispatcher,
): Promise<PackageStatus> {
  const result = await fetchReverseDependencies(pkg.name, dispatcher)

  if (isRubyGemsFetchError(result)) {
    if (result.kind === 'NOT_FOUND') {
      await updateRubyGemsDependentCount(qx, pkg.id, 0)
      return 'notFound'
    }
    log.warn({ name: pkg.name }, 'Rate limited fetching reverse dependencies — will retry')
    return 'error'
  }

  await updateRubyGemsDependentCount(qx, pkg.id, result.length)
  return 'processed'
}

export async function processBatch(
  qx: QueryExecutor,
  config: RubyGemsDependentsConfig,
): Promise<DependentsBatchResult> {
  const packages = await listRubyGemsPackagesForDependents(qx, { limit: config.batchSize })

  if (packages.length === 0) return { processed: 0, notFound: 0, error: 0 }

  const proxyUrls = rubyGemsProxyUrls()
  const dispatchers: Array<Dispatcher | undefined> = proxyUrls.length
    ? proxyUrls.map((url) => new ProxyAgent(url))
    : [undefined]

  log.info({ count: packages.length, proxies: proxyUrls.length }, 'Dependents batch started')

  const counts: DependentsBatchResult = { processed: 0, notFound: 0, error: 0 }

  try {
    for (let batchStart = 0; batchStart < packages.length; batchStart += config.concurrency) {
      const group = packages.slice(batchStart, batchStart + config.concurrency)

      await Promise.all(
        group.map(async (pkg, i) => {
          const dispatcher = dispatchers[(batchStart + i) % dispatchers.length]
          try {
            const status = await processPackage(qx, pkg, dispatcher)
            counts[status]++
          } catch (err) {
            const message = err instanceof Error ? err.message : String(err)
            log.error({ name: pkg.name, error: message }, 'Unexpected error fetching dependents')
            counts.error++
          }
        }),
      )

      const done = batchStart + group.length
      if (done % 1000 === 0 || done === packages.length) {
        log.info({ done, total: packages.length, ...counts }, 'Dependents progress')
      }
    }
  } finally {
    await Promise.all(dispatchers.map((d) => d?.close()))
  }

  return counts
}
