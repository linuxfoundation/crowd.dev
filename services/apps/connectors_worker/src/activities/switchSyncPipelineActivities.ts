import {
  activateSyncUnit,
  findSyncUnitByChannelName,
  getSyncDiffCountsForDays,
  isSyncUnitLocked,
  listShadowDiffUnits,
  pauseSyncUnit,
  setSyncUnitEmitEnabled,
  takeOverSyncUnit,
} from '@crowd/data-access-layer/src/connectors'
import {
  findActiveNangoMappingForRepo,
  getNangoModelLastCheckedAt,
} from '@crowd/data-access-layer/src/integrations'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import {
  NangoIntegration,
  getGithubSyncForConnectorSync,
  getNangoConnectionStatus,
  githubChannelName,
  initNangoCloudClient,
  pauseNangoSync,
  startNangoSync,
} from '@crowd/nango'

import { svc } from '../main'
import {
  evaluateBackUnitGate,
  evaluateDiffCleanliness,
  evaluateForwardUnitGate,
  evaluateNangoConsumption,
  groupUnitsForSweep,
  handoverPoint,
  handoverSince,
  recentDiffedDays,
  resolveSwitchTargets,
} from '../syncPipelineSwitch'
import type { IGateRefusal, ISweepRepo, ISwitchSyncTarget } from '../syncPipelineSwitch'

const PLATFORM = 'github'
const SWITCH_EVENT = 'sync_pipeline_switch'

export interface ISwitchRepoTarget {
  integrationId: string
  connectionId: string
  channelName: string
}

export interface IResolveSwitchTargetInput {
  owner: string
  repo: string
  syncNames?: string[]
}

export interface IResolveSwitchTargetResult {
  target: ISwitchRepoTarget | null
  syncs: ISwitchSyncTarget[]
}

export interface IUnitGateInput {
  integrationId: string
  connectionId: string
  channelName: string
  syncName: string
}

export type IUnitGateResult = { ok: true; unitId: string } | IGateRefusal

export interface INangoSyncInput {
  connectionId: string
  nangoSync: string
}

export interface ISetUnitEmitEnabledInput {
  unitId: string
  emitEnabled: boolean
}

export interface IUnitInput {
  unitId: string
}

export interface IHandoverPointInput {
  integrationId: string
  connectionId: string
  syncName: string
  nangoSync: string
}

export interface IHandoverPoint {
  handoverAt: string | null
}

export interface ITakeOverUnitInput {
  unitId: string
  handoverAt: string | null
}

export interface ITakeOverUnitResult {
  since: string | null
}

export async function resolveSwitchTarget(
  input: IResolveSwitchTargetInput,
): Promise<IResolveSwitchTargetResult> {
  const qx = dbStoreQx(svc.postgres.writer)
  const mapping = await findActiveNangoMappingForRepo(qx, input.owner, input.repo)
  const syncs = resolveSwitchTargets(input.syncNames)

  if (!mapping) {
    return { target: null, syncs }
  }

  return {
    target: {
      integrationId: mapping.integrationId,
      connectionId: mapping.connectionId,
      channelName: githubChannelName(mapping.owner, mapping.repoName),
    },
    syncs,
  }
}

export async function evaluateForwardGate(input: IUnitGateInput): Promise<IUnitGateResult> {
  const qx = dbStoreQx(svc.postgres.writer)
  const unit = await findSyncUnitByChannelName(
    qx,
    input.integrationId,
    PLATFORM,
    input.channelName,
    input.syncName,
  )

  const unitGate = evaluateForwardUnitGate(unit)
  if (unitGate.ok === false) {
    return unitGate
  }

  const lastCheckedAt = await getNangoModelLastCheckedAt(
    qx,
    input.integrationId,
    input.connectionId,
    getGithubSyncForConnectorSync(input.syncName).model,
  )
  const nangoGate = evaluateNangoConsumption(lastCheckedAt, new Date())
  if (nangoGate.ok === false) {
    return nangoGate
  }

  const days = recentDiffedDays(new Date())
  const rows = await getSyncDiffCountsForDays(qx, unit.id, days)
  const diffGate = evaluateDiffCleanliness(days, rows)
  if (diffGate.ok === false) {
    return diffGate
  }

  return { ok: true, unitId: unit.id }
}

export async function evaluateBackGate(input: IUnitGateInput): Promise<IUnitGateResult> {
  const qx = dbStoreQx(svc.postgres.writer)
  const unit = await findSyncUnitByChannelName(
    qx,
    input.integrationId,
    PLATFORM,
    input.channelName,
    input.syncName,
  )

  const gate = evaluateBackUnitGate(unit)
  if (gate.ok === false) {
    return gate
  }

  return { ok: true, unitId: unit.id }
}

export async function pauseNangoRepoSync(input: INangoSyncInput): Promise<void> {
  await initNangoCloudClient()
  await pauseNangoSync(NangoIntegration.GITHUB, input.connectionId, [input.nangoSync])
  svc.log.info({ event: SWITCH_EVENT, ...input }, 'nango sync paused')
}

export async function resumeNangoRepoSync(input: INangoSyncInput): Promise<void> {
  await initNangoCloudClient()
  await startNangoSync(NangoIntegration.GITHUB, input.connectionId, [input.nangoSync])
  svc.log.info({ event: SWITCH_EVENT, ...input }, 'nango sync resumed')
}

export async function setUnitEmitEnabled(input: ISetUnitEmitEnabledInput): Promise<void> {
  const qx = dbStoreQx(svc.postgres.writer)
  const updated = await setSyncUnitEmitEnabled(qx, input.unitId, input.emitEnabled)
  svc.log.info(
    { event: SWITCH_EVENT, ...input, alreadyInState: updated === 0 },
    'sync unit emit flag set',
  )
}

export async function quiesceUnit(input: IUnitInput): Promise<void> {
  const paused = await pauseSyncUnit(dbStoreQx(svc.postgres.writer), input.unitId)
  svc.log.info({ event: SWITCH_EVENT, ...input, paused: paused === 1 }, 'sync unit quiesced')
}

export async function isUnitLocked(input: IUnitInput): Promise<boolean> {
  return isSyncUnitLocked(dbStoreQx(svc.postgres.writer), input.unitId)
}

export async function activateUnit(input: IUnitInput): Promise<void> {
  const activated = await activateSyncUnit(dbStoreQx(svc.postgres.writer), input.unitId)
  svc.log.info({ event: SWITCH_EVENT, ...input, activated: activated === 1 }, 'sync unit activated')
}

export async function readNangoHandoverPoint(input: IHandoverPointInput): Promise<IHandoverPoint> {
  await initNangoCloudClient()
  const syncs = await getNangoConnectionStatus(NangoIntegration.GITHUB, input.connectionId)
  const sync = syncs?.find((s) => s.name === input.nangoSync)
  const nangoRunFinishedAt = sync?.finishedAt ?? null
  const consumedAt = await getNangoModelLastCheckedAt(
    dbStoreQx(svc.postgres.writer),
    input.integrationId,
    input.connectionId,
    getGithubSyncForConnectorSync(input.syncName).model,
  )
  const handoverAt = handoverPoint(nangoRunFinishedAt, consumedAt)
  svc.log.info(
    {
      event: SWITCH_EVENT,
      ...input,
      nangoStatus: sync?.status ?? null,
      nangoRunFinishedAt,
      consumedAt,
      handoverAt,
    },
    'nango handover point read',
  )
  return { handoverAt }
}

export async function takeOverUnit(input: ITakeOverUnitInput): Promise<ITakeOverUnitResult> {
  const since = input.handoverAt ? handoverSince(input.handoverAt) : null
  const flipped = await takeOverSyncUnit(dbStoreQx(svc.postgres.writer), input.unitId, since)
  svc.log.info(
    { event: SWITCH_EVENT, ...input, since, alreadyEmitting: flipped === 0 },
    'sync unit taken over',
  )
  return { since }
}

export async function listSwitchSweepRepos(): Promise<ISweepRepo[]> {
  const units = await listShadowDiffUnits(dbStoreQx(svc.postgres.writer))
  const repos = groupUnitsForSweep(units)
  svc.log.info(
    { event: SWITCH_EVENT, repos: repos.length, units: units.length },
    'sweep candidates listed',
  )
  return repos
}
