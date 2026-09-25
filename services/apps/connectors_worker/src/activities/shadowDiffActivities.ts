import type { ConnectorHttp } from '@crowd/connectors'
import { createHttpClient, createTokenPool, getCredential, getManifest } from '@crowd/connectors'
import { parseRepoChannel } from '@crowd/connectors/src/connectors/github/paging'
import {
  IShadowDiffUnit,
  getUnitIdsWithSummary,
  listShadowDiffUnits,
  pruneMatchingShadowRecords,
  upsertSyncDiffSummary,
} from '@crowd/data-access-layer/src/connectors'
import { getNangoMappingForRepo } from '@crowd/data-access-layer/src/integrations'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'

import { dropConfirmedForcePushedCommits, hasForcePushCandidates } from '../forcePushedCommits'
import { svc } from '../main'
import {
  IShadowDiffUnitResult,
  countMismatchesByKind,
  diffUnit,
  diffableRecordKey,
  resolveDiffWindow,
} from '../shadowDiffUnit'

const GITHUB_PLATFORM = 'github'

export interface IShadowDiffChannel {
  channelName: string
  integrationId: string
  units: IShadowDiffUnit[]
}

export type ShadowDiffChannelStatus = 'ok' | 'mapping_missing' | 'error'

export interface IShadowDiffChannelResult {
  channelName: string
  integrationId: string
  status: ShadowDiffChannelStatus
  errorMessage?: string
}

async function createGithubConfirmationHttp(
  qx: ReturnType<typeof dbStoreQx>,
  integrationId: string,
): Promise<ConnectorHttp> {
  const manifest = getManifest(GITHUB_PLATFORM)
  const credential = manifest.mintToken ? await getCredential(qx, integrationId) : null
  const pool = createTokenPool(svc.redis, GITHUB_PLATFORM, {
    probeBudget: manifest.probeBudget,
    mintToken: credential && manifest.mintToken ? manifest.mintToken(credential) : undefined,
    log: svc.log,
  })
  const preferredEntryId =
    credential && manifest.preparePool
      ? (await manifest.preparePool(credential, pool)).preferredEntryId
      : undefined

  return createHttpClient({
    acquireToken: () => pool.acquire(preferredEntryId),
    parkToken: pool.park,
    invalidateToken: pool.invalidate,
    interpretResponse: manifest.interpretResponse,
    log: svc.log,
  })
}

export async function listShadowDiffChannels(): Promise<IShadowDiffChannel[]> {
  const units = await listShadowDiffUnits(dbStoreQx(svc.postgres.writer))

  const byChannel = new Map<string, IShadowDiffChannel>()
  for (const unit of units) {
    const key = `${unit.integrationId}::${unit.channelName}`
    const existing = byChannel.get(key)
    if (existing) {
      existing.units.push(unit)
    } else {
      byChannel.set(key, {
        channelName: unit.channelName,
        integrationId: unit.integrationId,
        units: [unit],
      })
    }
  }

  return [...byChannel.values()]
}

async function persistUnitDiffResult(
  qx: ReturnType<typeof dbStoreQx>,
  channel: IShadowDiffChannel,
  unit: IShadowDiffUnit,
  day: string,
  windowStart: Date,
  windowEnd: Date,
  { mismatches, shadowKeys }: IShadowDiffUnitResult,
): Promise<void> {
  const counts = countMismatchesByKind(mismatches)

  await upsertSyncDiffSummary(qx, {
    unitId: unit.id,
    day,
    integrationId: channel.integrationId,
    channelName: channel.channelName,
    missingInNangoCount: counts.missing_in_nango,
    missingInShadowCount: counts.missing_in_shadow,
    fieldMismatchCount: counts.field_mismatch,
    unsupportedSyncCount: counts.unsupported_sync,
    highSeverityCount: mismatches.filter((m) => m.severity === 'high').length,
  })

  if (counts.unsupported_sync > 0) {
    return
  }

  const keysWithUnresolvedMismatch = new Set(
    mismatches
      .filter((m) => m.kind === 'field_mismatch' || m.kind === 'missing_in_nango')
      .map((m) => diffableRecordKey(m)),
  )

  const keysToDelete = shadowKeys.filter(
    (key) => !keysWithUnresolvedMismatch.has(diffableRecordKey(key)),
  )

  await pruneMatchingShadowRecords(qx, unit.id, windowStart, windowEnd, keysToDelete)
}

export async function runShadowDiffForChannel(
  channel: IShadowDiffChannel,
  targetDay?: string,
): Promise<IShadowDiffChannelResult> {
  const qx = dbStoreQx(svc.postgres.writer)

  const { owner, repo } = parseRepoChannel(channel.channelName)
  const mapping = await getNangoMappingForRepo(qx, channel.integrationId, owner, repo)

  if (!mapping) {
    return {
      channelName: channel.channelName,
      integrationId: channel.integrationId,
      status: 'mapping_missing',
    }
  }

  const { day, windowStart, windowEnd } = resolveDiffWindow(targetDay)

  const alreadySummarized = await getUnitIdsWithSummary(
    qx,
    channel.units.map((unit) => unit.id),
    day,
  )
  const pendingUnits = channel.units.filter((unit) => !alreadySummarized.has(unit.id))

  const unitDiffs: { unit: IShadowDiffUnit; result: IShadowDiffUnitResult }[] = []
  let confirmationHttp: Promise<ConnectorHttp> | null = null
  for (const unit of pendingUnits) {
    const result = await diffUnit(qx, unit, mapping.connectionId, windowStart, windowEnd)

    if (hasForcePushCandidates(unit.syncName, result.mismatches)) {
      confirmationHttp ??= createGithubConfirmationHttp(qx, channel.integrationId)
      let http: ConnectorHttp
      try {
        http = await confirmationHttp
      } catch (err) {
        svc.log.warn(
          { err, unitId: unit.id, day, channelName: channel.channelName },
          'failed to set up github client for force-push confirmation, keeping candidates as missing_in_shadow',
        )
        unitDiffs.push({ unit, result })
        continue
      }
      const { mismatches, skippedCount, failedCount } = await dropConfirmedForcePushedCommits(
        unit.syncName,
        result.mismatches,
        owner,
        repo,
        http,
        svc.log,
      )
      if (skippedCount > 0) {
        svc.log.info(
          { unitId: unit.id, day, channelName: channel.channelName, skippedCount },
          'skipped force-pushed commits confirmed missing from github',
        )
      }
      if (failedCount > 0) {
        svc.log.warn(
          { unitId: unit.id, day, channelName: channel.channelName, failedCount },
          'failed to confirm force-pushed commit candidates against github, keeping them as missing_in_shadow',
        )
      }
      unitDiffs.push({ unit, result: { ...result, mismatches } })
      continue
    }

    unitDiffs.push({ unit, result })
  }

  await qx.tx(async (txQx) => {
    for (const { unit, result } of unitDiffs) {
      await persistUnitDiffResult(txQx, channel, unit, day, windowStart, windowEnd, result)
    }
  })

  return {
    channelName: channel.channelName,
    integrationId: channel.integrationId,
    status: 'ok',
  }
}
