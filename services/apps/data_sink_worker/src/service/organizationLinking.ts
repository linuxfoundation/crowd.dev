import uniqby from 'lodash.uniqby'

import { normalizeHostname } from '@crowd/common'
import { IFindOrCreateOrganizationResult } from '@crowd/data-access-layer'
import { DbStore } from '@crowd/data-access-layer/src/database'
import { Logger, logExecutionTimeV2 } from '@crowd/logging'
import { Client as TemporalClient } from '@crowd/temporal'
import {
  IOrganization,
  IOrganizationIdSource,
  MemberIdentityType,
  OrganizationIdentityType,
  OrganizationSource,
  PlatformType,
} from '@crowd/types'

import { ActivityTimestampsByEmailDomain, IMemberUpdateData } from './member.data'
import type MemberService from './member.service'
import { OrganizationService } from './organization.service'
import { IOrgLookupCache } from './orgLookupCache'

export type OrgPromiseCache = Map<string, Promise<IFindOrCreateOrganizationResult | undefined>>

export interface IMemberOrganizationsLink {
  step: 'create' | 'update'
  memberId: string
  segmentIds: string[]
  integrationId: string
  platform: PlatformType
  data: Pick<IMemberUpdateData, 'organizations' | 'identities'>
  orgPromiseCache?: OrgPromiseCache
  activityTimestamp?: string | ActivityTimestampsByEmailDomain
  isBotMember: boolean
}

export type EmailDomainOrganization = {
  id: string
  source: OrganizationSource
  created: boolean
}

// Domains after hostname normalization, like findOrCreateOrganization looks them up. The lock key
// takes a primary domain on any platform, as the lookup's fallback does
function verifiedOrgIdentityKey(
  identity: NonNullable<IOrganization['identities']>[number],
): { key: string; lockKey: string; value: string } | undefined {
  if (!identity.verified || !identity.value) {
    return undefined
  }
  const isDomain = [
    OrganizationIdentityType.PRIMARY_DOMAIN,
    OrganizationIdentityType.ALTERNATIVE_DOMAIN,
  ].includes(identity.type)
  const value = isDomain ? normalizeHostname(identity.value, false) : identity.value
  if (!value) {
    return undefined
  }
  const key = `${identity.platform}:${identity.type}:${value.toLowerCase()}`
  return {
    value,
    key,
    lockKey:
      identity.type === OrganizationIdentityType.PRIMARY_DOMAIN
        ? `${identity.type}:${value.toLowerCase()}`
        : key,
  }
}

/**
 * Returns a stable cache key for an org based on its verified identities, falling back to
 * displayName. Used by the org promise cache to deduplicate `findOrCreateOrganization` calls
 * across members in the same batch.
 */
function orgCacheKey(org: IOrganization): string | null {
  const verified = Array.from(
    new Set((org.identities ?? []).map((i) => verifiedOrgIdentityKey(i)?.key).filter(Boolean)),
  )
    .sort()
    .join('|')
  if (verified) return verified
  if (org.displayName) return `name:${org.displayName.toLowerCase()}`
  return null
}

// Per batch (through its org promise cache): the latest find-or-create call for each verified
// identity. A call waits for the earlier ones that share an identity with it, so it finds the
// organization they created instead of creating a duplicate when members are updated concurrently.
const orgIdentityLocks = new WeakMap<OrgPromiseCache, Map<string, Promise<unknown>>>()

// The verified identities, and the name as the fallback. Without a display name, an organization
// is created under its first verified identity's value.
function orgLockKeys(org: IOrganization): string[] {
  const keys: string[] = []
  let creationName: string | undefined
  for (const identity of org.identities ?? []) {
    const verified = verifiedOrgIdentityKey(identity)
    if (!verified) {
      continue
    }
    creationName ??= verified.value
    keys.push(verified.lockKey)
  }

  const name = (org.displayName || creationName)?.trim().toLowerCase()
  if (name) {
    keys.push(`name:${name}`)
  }
  return keys
}

// Per batch: the latest update of each existing organization. Calls with different identities can
// resolve to the same organization (so the identity locks don't order them), and the data sink
// has no transactions, so they update it one at a time.
const orgIdLocks = new WeakMap<OrgPromiseCache, Map<string, Promise<void>>>()

function lockOrganizationFor(orgPromiseCache: OrgPromiseCache) {
  return async (organizationId: string): Promise<() => void> => {
    const locks = orgIdLocks.get(orgPromiseCache) ?? new Map<string, Promise<void>>()
    orgIdLocks.set(orgPromiseCache, locks)
    const previous = locks.get(organizationId) ?? Promise.resolve()
    let release: () => void = () => undefined
    const held = new Promise<void>((resolve) => {
      release = resolve
    })
    locks.set(
      organizationId,
      previous.then(() => held),
    )
    await previous
    return release
  }
}

const stableJson = (value: unknown): string =>
  JSON.stringify(value, (_key, v) =>
    v && typeof v === 'object' && !Array.isArray(v)
      ? Object.fromEntries(Object.entries(v).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
      : v,
  )

// Domains are normalized like findOrCreateOrganization normalizes them in place, so the key is the
// same before and after the payload is applied
function orgPayloadKey(source: string, org: IOrganization): string {
  const content: Record<string, unknown> = {
    ...org,
    identities: (org.identities ?? [])
      .map((i) =>
        [
          OrganizationIdentityType.PRIMARY_DOMAIN,
          OrganizationIdentityType.ALTERNATIVE_DOMAIN,
        ].includes(i.type)
          ? { ...i, value: i.value ? normalizeHostname(i.value, false) : i.value }
          : i,
      )
      .filter((i) => i.value !== undefined),
  }
  // the source of the member's link to the organization, not organization data
  delete content.source
  return stableJson({ source, content })
}

interface IOrgPayloadActivities {
  newestMs: number
  count: number
}

interface IAppliedOrgPayload {
  organizationId: string
  source: string
  payloadKey: string
  activities: IOrgPayloadActivities
  apply: () => Promise<unknown>
}

const orgPayloadsOfBatch = new WeakMap<
  OrgPromiseCache,
  { activities: Map<string, IOrgPayloadActivities>; applied: IAppliedOrgPayload[] }
>()

function batchOrgPayloads(orgPromiseCache: OrgPromiseCache) {
  let payloads = orgPayloadsOfBatch.get(orgPromiseCache)
  if (!payloads) {
    payloads = { activities: new Map(), applied: [] }
    orgPayloadsOfBatch.set(orgPromiseCache, payloads)
  }
  return payloads
}

export function registerOrgPayloadActivities(
  orgPromiseCache: OrgPromiseCache,
  activities: { source: string; timestamp: string; organizations?: IOrganization[] }[],
): void {
  const payloads = batchOrgPayloads(orgPromiseCache).activities
  for (const { source, timestamp, organizations } of activities) {
    const time = new Date(timestamp).getTime()
    const ms = Number.isFinite(time) ? time : 0
    let keys: Set<string>
    try {
      // an activity counts once for each payload, however many times it carries it
      keys = new Set((organizations ?? []).map((org) => orgPayloadKey(source, org)))
    } catch {
      // runs before the per-member error handling, which reports a malformed organization
      continue
    }
    for (const key of keys) {
      const known = payloads.get(key)
      payloads.set(key, {
        newestMs: Math.max(known?.newestMs ?? ms, ms),
        count: (known?.count ?? 0) + 1,
      })
    }
  }
}

const winsOver = (a: IAppliedOrgPayload, b: IAppliedOrgPayload): boolean =>
  a.activities.newestMs !== b.activities.newestMs
    ? a.activities.newestMs > b.activities.newestMs
    : a.activities.count !== b.activities.count
      ? a.activities.count > b.activities.count
      : a.payloadKey > b.payloadKey

// The payload applied last sets the defaults of its source, and which one that is depends on timing,
// so they are applied again from lowest ranked to the winner (newest activity, most activities)
export async function applyNewestOrgPayloads(
  orgPromiseCache: OrgPromiseCache,
  log: Logger,
): Promise<void> {
  const byOrganizationAndSource = new Map<string, IAppliedOrgPayload[]>()
  for (const payload of batchOrgPayloads(orgPromiseCache).applied) {
    const key = `${payload.organizationId}#${payload.source}`
    const payloads = byOrganizationAndSource.get(key) ?? []
    payloads.push(payload)
    byOrganizationAndSource.set(key, payloads)
  }

  for (const payloads of byOrganizationAndSource.values()) {
    const distinct = Array.from(new Map(payloads.map((p) => [p.payloadKey, p])).values())
    if (distinct.length < 2) {
      continue
    }
    distinct.sort((a, b) => (winsOver(a, b) ? 1 : winsOver(b, a) ? -1 : 0))
    for (const payload of distinct) {
      try {
        await payload.apply()
      } catch (err) {
        log.error(
          err,
          { organizationId: payload.organizationId, source: payload.source },
          'Failed to apply an organization payload again!',
        )
      }
    }
  }
}

// Per batch: the organization each identity key resolved to. Later payloads with the same identities
// go there without a lookup: an earlier payload may have unverified their identity
const orgIdsOfIdentityKeys = new WeakMap<OrgPromiseCache, Map<string, string>>()

function registerAppliedOrgPayload(
  orgPromiseCache: OrgPromiseCache,
  payload: Omit<IAppliedOrgPayload, 'activities'>,
): void {
  const payloads = batchOrgPayloads(orgPromiseCache)
  payloads.applied.push({
    ...payload,
    activities: payloads.activities.get(payload.payloadKey) ?? { newestMs: 0, count: 0 },
  })
}

type OrganizationLock = (organizationId: string) => Promise<(() => void) | undefined>

// Payloads that share identities wait for each other, so they all land on the same organization
// and each adds what it carries. A refresh looks the organization up again, skipping every cache
export function findOrCreateOrgOnce(
  lookups: IOrgLookupCache,
  orgPromiseCache: OrgPromiseCache | undefined,
  source: string,
  org: IOrganization,
  findOrCreate: (
    lockOrganization?: OrganizationLock,
    organizationId?: string,
  ) => Promise<IFindOrCreateOrganizationResult | undefined>,
  refresh = false,
): Promise<IFindOrCreateOrganizationResult | undefined> {
  const payloadKey = orgPayloadKey(source, org)
  const identityKey = orgCacheKey(org)
  const key = identityKey ? `${identityKey}#${payloadKey}` : null
  const cached = key && !refresh ? orgPromiseCache?.get(key) : undefined
  if (cached) {
    return cached
  }
  // A lookup by name can change without a write to the organization it found, so only lookups by
  // verified identities are shared across pods
  const sharedKey = key && !identityKey.startsWith('name:') ? key : null

  // Without transactions the first statements of a write that fails stay, so it is marked too
  const run = async (
    lockOrganization?: OrganizationLock,
    pinnedId?: string,
    startedAt?: number,
  ): Promise<IFindOrCreateOrganizationResult | undefined> => {
    const shared = pinnedId ? null : sharedKey
    const clock = shared && startedAt === undefined ? await lookups.start() : startedAt
    let writing: string | undefined
    const lock = async (id: string) => {
      writing = id
      return lockOrganization?.(id)
    }

    let result: IFindOrCreateOrganizationResult | undefined
    try {
      result = await findOrCreate(lock, pinnedId)
    } catch (err) {
      if (writing) {
        await lookups.markChanged([writing])
      }
      throw err
    }

    if (result && shared) {
      await lookups.remember(shared, result, clock)
    } else if (result?.changed) {
      await lookups.markChanged([result.id])
    }
    return result
  }

  const lookUp = async (
    lockOrganization?: OrganizationLock,
    resolvedId?: string,
  ): Promise<IFindOrCreateOrganizationResult | undefined> => {
    const { organizationId: cachedId, startedAt } =
      sharedKey && !refresh ? await lookups.find(sharedKey) : {}
    const cacheAgreesWithBatch = !resolvedId || resolvedId === cachedId
    if (cachedId && cacheAgreesWithBatch) {
      return { id: cachedId, created: false, changed: false }
    }
    const result = await run(lockOrganization, resolvedId, startedAt)
    if (result || !resolvedId) {
      return result
    }
    // the organization this batch resolved the identities to is gone, e.g. merged away
    await lookups.markChanged([resolvedId])
    return run(lockOrganization, undefined, startedAt)
  }

  if (!orgPromiseCache) {
    return lookUp()
  }

  const locks = orgIdentityLocks.get(orgPromiseCache) ?? new Map<string, Promise<unknown>>()
  orgIdentityLocks.set(orgPromiseCache, locks)
  const lockKeys = orgLockKeys(org)
  const resolvedIds = orgIdsOfIdentityKeys.get(orgPromiseCache) ?? new Map<string, string>()
  orgIdsOfIdentityKeys.set(orgPromiseCache, resolvedIds)
  const promise = Promise.all(lockKeys.map((k) => locks.get(k)))
    .then(() =>
      lookUp(
        lockOrganizationFor(orgPromiseCache),
        identityKey ? resolvedIds.get(identityKey) : undefined,
      ),
    )
    .then((result) => {
      if (result) {
        if (identityKey && (refresh || resolvedIds.get(identityKey) !== result.id)) {
          resolvedIds.set(identityKey, result.id)
        }
        registerAppliedOrgPayload(orgPromiseCache, {
          organizationId: result.id,
          source,
          payloadKey,
          apply: () => run(lockOrganizationFor(orgPromiseCache), result.id),
        })
      }
      return result
    })
  const settled = promise.catch(() => undefined)
  for (const k of lockKeys) {
    locks.set(k, settled)
  }

  if (key) {
    orgPromiseCache.set(key, promise)
    promise.catch(() => {
      if (orgPromiseCache.get(key) === promise) {
        orgPromiseCache.delete(key)
      }
    })
  }
  return promise
}

const isMissingOrganizationError = (err: unknown): boolean =>
  (err as { code?: string })?.code === '23503' &&
  String((err as { detail?: string }).detail).includes('table "organizations"')

const missingOrganizationId = (err: unknown): string | undefined =>
  String((err as { detail?: string }).detail).match(/=\(([0-9a-f-]{36})\)/)?.[1]

export interface IOrganizationLinkDeps {
  store: DbStore
  temporal: TemporalClient
  log: Logger
  orgLookups: IOrgLookupCache
  assignByEmailDomain: MemberService['assignOrganizationByEmailDomain']
}

// Without transactions, an organization taken from a cache can be merged away before the member is
// linked to it
export async function linkMemberOrganizations(
  deps: IOrganizationLinkDeps,
  link: IMemberOrganizationsLink,
): Promise<void> {
  // The retry finds what the first attempt created as existing
  const createdIds = new Set<string>()
  try {
    await linkMemberOrganizationsOnce(deps, link, false, createdIds)
  } catch (err) {
    if (!isMissingOrganizationError(err)) {
      throw err
    }
    deps.log.warn(
      { memberId: link.memberId },
      'Organization to link no longer exists, looking organizations up again!',
    )
    await linkMemberOrganizationsOnce(deps, link, true, createdIds)
  }
}

async function linkMemberOrganizationsOnce(
  { log, orgLookups, assignByEmailDomain, store, temporal }: IOrganizationLinkDeps,
  {
    step,
    memberId,
    segmentIds,
    integrationId,
    platform,
    data,
    orgPromiseCache,
    activityTimestamp,
    isBotMember,
  }: IMemberOrganizationsLink,
  refresh: boolean,
  createdIds: Set<string>,
): Promise<void> {
  const organizations: IOrganizationIdSource[] = []
  const orgService = new OrganizationService(store, temporal, log)
  for (const org of data.organizations ?? []) {
    // Temp fix: skip the individual-noaccount.com placeholder org to avoid
    // hot-row contention on the organizations table. Permanent fix is in
    // tncTransformerBase.ts to stop emitting this org entirely.
    if (org.identities?.some((i) => i.verified && i.value === 'individual-noaccount.com')) {
      continue
    }

    log.trace({ memberId }, 'Finding or creating organization!')

    const result = await findOrCreateOrgOnce(
      orgLookups,
      orgPromiseCache,
      platform,
      org,
      (lockOrganization, organizationId) =>
        logExecutionTimeV2(
          () =>
            orgService.findOrCreate(platform, integrationId, org, lockOrganization, organizationId),
          log,
          `memberService -> ${step} -> orgService.findOrCreate`,
        ),
      refresh,
    )
    if (result) {
      organizations.push({
        id: result.id,
        source: org.source,
      })
    }
  }

  const emailIdentities = data.identities.filter(
    (i) => i.verified && i.type === MemberIdentityType.EMAIL,
  )
  let fromEmailDomain: EmailDomainOrganization[] = []
  if (emailIdentities.length > 0) {
    log.trace({ memberId }, 'Assigning organization by email domain!')
    fromEmailDomain = await logExecutionTimeV2(
      () =>
        assignByEmailDomain(
          integrationId,
          emailIdentities.map((i) => i.value),
          orgPromiseCache,
          memberId,
          activityTimestamp,
          isBotMember,
          refresh,
          createdIds,
        ),
      log,
      `memberService -> ${step} -> assignOrganizationByEmailDomain`,
    )
    organizations.push(...fromEmailDomain)
  }

  if (organizations.length === 0) {
    return
  }

  log.trace({ memberId }, 'Finding member organizations!')
  const orgsToAdd = (
    await Promise.all(
      uniqby(organizations, 'id').map(async (org) => {
        // Check if the org was already added to the member in the past, including deleted ones.
        // If it was, we ignore this org to prevent from adding it again.
        const existingMemberOrgs = await logExecutionTimeV2(
          () => orgService.findMemberOrganizations(memberId, org.id),
          log,
          `memberService -> ${step} -> orgService.findMemberOrganizations`,
        )
        return existingMemberOrgs.length > 0 ? null : org
      }),
    )
  ).filter((org) => org !== null)

  if (orgsToAdd.length === 0) {
    return
  }

  log.trace({ memberId }, 'Adding organizations to member!')
  try {
    await logExecutionTimeV2(
      () => orgService.addToMember(segmentIds, memberId, orgsToAdd),
      log,
      `memberService -> ${step} -> orgService.addToMember`,
    )
  } catch (err) {
    if (isMissingOrganizationError(err)) {
      const missingId = missingOrganizationId(err)
      await orgLookups.markChanged(missingId ? [missingId] : orgsToAdd.map((org) => org.id))
    }
    throw err
  }

  const addedIds = new Set(orgsToAdd.map((org) => org.id))
  for (const org of fromEmailDomain) {
    if (createdIds.has(org.id) && addedIds.has(org.id)) {
      await orgService.startFakeOrganizationAnalysisWorkflow(org.id)
    }
  }
}
