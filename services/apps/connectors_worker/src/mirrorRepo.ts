import axios from 'axios'

import { timeout } from '@crowd/common'
import { getCredential } from '@crowd/connectors'
import { githubConnector } from '@crowd/connectors/src/connectors/github'
import { mintInstallationToken } from '@crowd/connectors/src/connectors/github/appToken'
import { resolveRepoChannel } from '@crowd/connectors/src/connectors/github/discover'
import {
  alignSyncUnitChannelName,
  hasSyncUnitsForChannel,
  upsertSyncUnits,
} from '@crowd/data-access-layer/src/connectors'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { findIntegrationRepositoryUrl } from '@crowd/data-access-layer/src/repositories'
import { NangoIntegration, getNangoConnectionData, githubChannelName } from '@crowd/nango'

import {
  type IMirrorCandidate,
  type MirrorRepoResult,
  type MirrorSkipReason,
  type MirrorStage,
  isRenamedChannel,
  mirrorSkipReasonFor,
} from './mirrorSweep'

interface IRepoChannel {
  channelId: string
  channelName: string
}

const NANGO_RETRY_DELAYS_MS = [5_000, 15_000, 45_000]
const NANGO_REQUEST_TIMEOUT_MS = 30_000

class NangoRequestTimeout extends Error {}

class MirrorSkip extends Error {
  constructor(
    readonly reason: MirrorSkipReason,
    readonly detail?: string,
  ) {
    super(reason)
  }
}

async function atStage<T>(stage: MirrorStage, run: () => Promise<T>): Promise<T> {
  try {
    return await run()
  } catch (err) {
    throw new MirrorSkip(
      mirrorSkipReasonFor(stage, err),
      err instanceof Error ? err.message : undefined,
    )
  }
}

const isTransientNangoError = (err: unknown): boolean =>
  err instanceof NangoRequestTimeout ||
  (axios.isAxiosError(err) && (err.response === undefined || err.response.status >= 500))

async function getNangoConnectionWithin(
  connectionId: string,
): ReturnType<typeof getNangoConnectionData> {
  let timer: NodeJS.Timeout | undefined
  const timedOut = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () =>
        reject(
          new NangoRequestTimeout(`nango request timed out after ${NANGO_REQUEST_TIMEOUT_MS} ms`),
        ),
      NANGO_REQUEST_TIMEOUT_MS,
    )
  })
  try {
    return await Promise.race([
      getNangoConnectionData(NangoIntegration.GITHUB, connectionId),
      timedOut,
    ])
  } finally {
    clearTimeout(timer)
  }
}

async function fetchNangoConnection(
  connectionId: string,
): ReturnType<typeof getNangoConnectionData> {
  for (const delayMs of NANGO_RETRY_DELAYS_MS) {
    try {
      return await getNangoConnectionWithin(connectionId)
    } catch (err) {
      if (!isTransientNangoError(err)) {
        throw err
      }
      await timeout(delayMs)
    }
  }
  return getNangoConnectionWithin(connectionId)
}

async function resolveCandidateChannel(
  qx: QueryExecutor,
  candidate: IMirrorCandidate,
): Promise<IRepoChannel> {
  const credential = await getCredential(qx, candidate.integrationId)
  const connection = await atStage('nango', () => fetchNangoConnection(candidate.connectionId))
  const installationId: string | undefined = connection.connection_config?.installation_id
  if (!installationId) {
    throw new MirrorSkip('no_installation_id')
  }
  const { token } = await atStage('mint', () => mintInstallationToken(credential, installationId))
  return atStage('resolve', () => resolveRepoChannel(token, candidate.owner, candidate.repoName))
}

async function storedChannelName(
  qx: QueryExecutor,
  candidate: IMirrorCandidate,
  htmlUrl: string,
): Promise<string> {
  return (
    (await findIntegrationRepositoryUrl(qx, candidate.integrationId, htmlUrl)) ??
    githubChannelName(candidate.owner, candidate.repoName)
  )
}

export async function mirrorRepo(
  qx: QueryExecutor,
  candidate: IMirrorCandidate,
  since: string,
): Promise<MirrorRepoResult> {
  let channel: IRepoChannel
  try {
    channel = await resolveCandidateChannel(qx, candidate)
  } catch (err) {
    if (err instanceof MirrorSkip) {
      return err.detail === undefined
        ? { outcome: 'skipped', reason: err.reason }
        : { outcome: 'skipped', reason: err.reason, message: err.detail }
    }
    throw err
  }

  if (isRenamedChannel(candidate, channel.channelName)) {
    return { outcome: 'skipped', reason: 'repo_renamed' }
  }
  const channelName = await storedChannelName(qx, candidate, channel.channelName)
  if (await hasSyncUnitsForChannel(qx, candidate.integrationId, channel.channelId)) {
    await alignSyncUnitChannelName(qx, candidate.integrationId, channel.channelId, channelName)
    return {
      outcome: 'skipped',
      reason: 'already_mirrored',
      message: `units exist for ${channel.channelName}`,
    }
  }

  const watermark = { phase: 'incremental', since, cursor: null }
  const unitsInserted = await upsertSyncUnits(
    qx,
    githubConnector.syncs.map(({ name }) => ({
      integrationId: candidate.integrationId,
      platform: githubConnector.platform,
      channelId: channel.channelId,
      channelName,
      syncName: name,
      watermark,
    })),
  )
  return { outcome: 'mirrored', unitsInserted }
}
