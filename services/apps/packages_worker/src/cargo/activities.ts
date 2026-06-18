import { rm } from 'node:fs/promises'

import { getServiceChildLogger } from '@crowd/logging'

import { getCargoConfig } from '../config'
import { getPackagesDb, getPackagesDbConnection } from '../db'

import { DUMP_DIR, downloadAndExtractDump } from './dump'
import {
  enrichDownloadsDaily,
  enrichMaintainers,
  enrichPackages,
  enrichRepos,
  enrichVersions,
  flushAudit,
} from './enrich'
import { STAGING_SCHEMA, loadDump } from './loadDump'
import {
  EnrichDownloadsDailyResult,
  EnrichMaintainersResult,
  EnrichPackagesResult,
  EnrichReposResult,
  EnrichVersionsResult,
  LoadResult,
} from './types'

const log = getServiceChildLogger('cargo-activity')

// Downloads + extracts the crates.io dump, then stages and denormalizes it into
// the cargo_sync schema. Config is read here (not at module load) so the
// workflow bundle can import this file for type discovery without env set.
export async function cargoDownloadAndLoad(): Promise<LoadResult> {
  const { dumpUrl } = getCargoConfig()
  const dumpDir = await downloadAndExtractDump(dumpUrl)
  const qx = await getPackagesDb()
  const conn = await getPackagesDbConnection()
  const result = await loadDump(qx, conn, dumpDir)
  log.info({ ...result }, 'cargo dump loaded')
  return result
}

export async function cargoEnrichPackages(): Promise<EnrichPackagesResult> {
  return enrichPackages(await getPackagesDb())
}

export async function cargoEnrichVersions(): Promise<EnrichVersionsResult> {
  return enrichVersions(await getPackagesDb())
}

export async function cargoEnrichRepos(): Promise<EnrichReposResult> {
  return enrichRepos(await getPackagesDb())
}

export async function cargoEnrichMaintainers(): Promise<EnrichMaintainersResult> {
  return enrichMaintainers(await getPackagesDb())
}

export async function cargoEnrichDownloadsDaily(): Promise<EnrichDownloadsDailyResult> {
  return enrichDownloadsDaily(await getPackagesDb())
}

export async function cargoFlushAudit(): Promise<number> {
  return flushAudit(await getPackagesDb())
}

// Best-effort teardown: drops the staging schema and removes the extracted dump.
// A crashed run is self-healing anyway — loadDump rebuilds the schema and dump.ts
// clears DUMP_DIR on the next run.
export async function cargoCleanup(): Promise<void> {
  const qx = await getPackagesDb()
  await qx.result(`DROP SCHEMA IF EXISTS ${STAGING_SCHEMA} CASCADE`)
  await rm(DUMP_DIR, { recursive: true, force: true })
  log.info('cargo cleanup complete')
}
