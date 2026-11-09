import isEqual from 'lodash.isequal'
import mergeWith from 'lodash.mergewith'
import uniqby from 'lodash.uniqby'

import {
  ApplicationError,
  DEFAULT_TENANT_ID,
  getAttributeValue,
  getCountry,
  hasAttributeValue,
  isDomainExcluded,
  isObjectEmpty,
  isSameMemberIdentity,
  normalizeDisplayName,
  normalizeHostname,
  normalizeMemberIdentities,
  omitUrlValues,
  singleOrDefault,
} from '@crowd/common'
import {
  BotDetectionService,
  CommonMemberService,
  MEMBER_ORG_STINT_CHANGES_DATES_PREFIX,
  MEMBER_ORG_STINT_CHANGES_QUEUE,
} from '@crowd/common_services'
import {
  IFindOrCreateOrganizationResult,
  QueryExecutor,
  createMember,
  dbStoreQx,
  updateMember,
} from '@crowd/data-access-layer'
import {
  findIdentitiesForMembers,
  findMembersByIdentities,
  findMembersByVerifiedUsernames,
} from '@crowd/data-access-layer'
import { DbStore } from '@crowd/data-access-layer/src/database'
import { getMemberNoMerge } from '@crowd/data-access-layer/src/member_merge'
import IntegrationRepository from '@crowd/data-access-layer/src/old/apps/data_sink_worker/repo/integration.repo'
import {
  IDbMember,
  IDbMemberUpdateData,
} from '@crowd/data-access-layer/src/old/apps/data_sink_worker/repo/member.data'
import MemberRepository from '@crowd/data-access-layer/src/old/apps/data_sink_worker/repo/member.repo'
import { Logger, LoggerBase, getChildLogger, logExecutionTimeV2 } from '@crowd/logging'
import { RedisClient } from '@crowd/redis'
import { Client as TemporalClient } from '@crowd/temporal'
import {
  IMemberData,
  IMemberIdentity,
  IOrganization,
  IOrganizationIdSource,
  MemberAttributeName,
  MemberBotDetection,
  MemberIdentityType,
  OrganizationAttributeSource,
  OrganizationIdentityType,
  OrganizationSource,
  PlatformType,
  TemporalWorkflowId,
} from '@crowd/types'

import {
  ActivityTimestampsByEmailDomain,
  IMemberCreateData,
  IMemberUpdateData,
} from './member.data'
import MemberAttributeService from './memberAttribute.service'
import { OrganizationService } from './organization.service'
import { IOrgLookupCache, createOrgLookupCache } from './orgLookupCache'

type OrgPromiseCache = Map<string, Promise<IFindOrCreateOrganizationResult | undefined>>

interface IMemberOrganizationsLink {
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

type EmailDomainOrganization = {
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
function findOrCreateOrgOnce(
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
        identityKey && !refresh ? resolvedIds.get(identityKey) : undefined,
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

// Integrations stamp activities without a date at 1970-01-01 (e.g. Slack channel members), which is
// never a real join date, so a date this close to the epoch never wins
const JOINED_AT_PLACEHOLDER_UNTIL = Date.UTC(1970, 0, 6)

export function getEarliestMemberJoinedAt(current: Date, incoming: Date): Date {
  const isPlaceholder = (date: Date) =>
    Number.isNaN(date.getTime()) || date.getTime() < JOINED_AT_PLACEHOLDER_UNTIL

  if (Number.isNaN(incoming.getTime())) {
    return current
  }
  if (isPlaceholder(current)) {
    return incoming
  }
  if (isPlaceholder(incoming)) {
    return current
  }
  return incoming.getTime() < current.getTime() ? incoming : current
}

/**
 * Checks the memberNoMerge table and, if allowed, merges secondaryId into primaryId
 * using CommonMemberService. Returns true if the merge was performed, false if a noMerge
 * record prevents it. Throws if the merge itself fails.
 */
export async function mergeIfAllowed(
  pgQx: QueryExecutor,
  temporal: TemporalClient,
  log: Logger,
  primaryId: string,
  secondaryId: string,
): Promise<boolean> {
  const noMergeMemberIds = await getMemberNoMerge(pgQx, [primaryId, secondaryId], {
    includeExpired: true,
  })
  const noMerge = noMergeMemberIds.some(
    (m) =>
      (m.memberId === primaryId && m.noMergeId === secondaryId) ||
      (m.memberId === secondaryId && m.noMergeId === primaryId),
  )
  if (noMerge) {
    log.warn({ primaryId, secondaryId }, 'Members are marked as no-merge — skipping merge')
    return false
  }
  await pgQx.tx(async (txPgQx) => {
    const service = new CommonMemberService(txPgQx, temporal, log)
    await service.merge(primaryId, secondaryId)
  })
  return true
}

export default class MemberService extends LoggerBase {
  private readonly memberRepo: MemberRepository
  private readonly pgQx: QueryExecutor
  private readonly botDetectionService: BotDetectionService
  private readonly orgLookups: IOrgLookupCache

  constructor(
    private readonly store: DbStore,
    private readonly redisClient: RedisClient,
    private readonly temporal: TemporalClient,
    parentLog: Logger,
  ) {
    super(parentLog)

    this.memberRepo = new MemberRepository(store, this.log)
    this.botDetectionService = new BotDetectionService(this.log)
    this.orgLookups = createOrgLookupCache(redisClient, this.log)
    this.pgQx = dbStoreQx(this.store)
  }

  /**
   * Inserts identities with failOnConflict=true. On a unique constraint violation: finds the
   * owner and, when attemptMerge=true (update path), attempts mergeIfAllowed. On merge success
   * returns the owner's memberId as a redirect signal. On failure re-throws the original
   * DatabaseError so handleMemberIdentityError in activity.service.ts can extract metadata and
   * write to integration.results.error. When attemptMerge=false (create path) the orphan has no
   * data (INSERT was atomic — nothing committed), so we skip the full merge and just return the
   * owner; the caller handles orphan cleanup via scheduleOrphanMemberDeletion.
   */
  private async insertIdentitiesWithConflictResolution(
    memberId: string,
    integrationId: string,
    identities: IMemberIdentity[],
    attemptMerge = true,
  ): Promise<string | void> {
    // Deduplicate by (platform, type, lower(value)) — prefer verified=true when the same
    // identity appears with both flags. Prevents per-member unique identity index
    // from firing when a payload contains the same identity twice with different verified values.
    const deduped = normalizeMemberIdentities(identities)

    // Skip unverified rows another member already has (same as enrichment).
    const unverified = deduped.filter((identity) => !identity.verified)
    const owners =
      unverified.length > 0
        ? await findMembersByIdentities(this.pgQx, unverified, memberId)
        : new Map<string, string>()

    const toInsert = deduped.filter(
      (identity) =>
        identity.verified ||
        !owners.has(`${identity.platform}:${identity.type}:${identity.value.trim()}`),
    )

    if (toInsert.length === 0) {
      return
    }

    try {
      await this.memberRepo.insertIdentities(memberId, integrationId, toInsert, true)
    } catch (err) {
      if (
        !err?.constraint ||
        (err.constraint !== 'uix_memberIdentities_platform_value_type_verified' &&
          err.constraint !== 'uix_memberIdentities_platform_type_lower_value_verified')
      ) {
        throw err
      }

      const verifiedIncoming = toInsert.filter((i) => i.verified)
      if (verifiedIncoming.length === 0) throw err

      // Use the structured identities array to find the owner — avoids fragile Postgres
      // Detail text parsing (format not stable; breaks if value contains a comma).
      const owners = await findMembersByIdentities(this.pgQx, verifiedIncoming, undefined, true)

      // Map keys are `${platform}:${type}:${value}` (from db rows). Match case-insensitively.
      let owner: string | undefined
      outer: for (const id of verifiedIncoming) {
        for (const [key, ownerId] of owners) {
          const sep1 = key.indexOf(':')
          const sep2 = key.indexOf(':', sep1 + 1)
          if (sep1 < 0 || sep2 < 0) continue
          if (
            key.slice(0, sep1) === id.platform &&
            key.slice(sep1 + 1, sep2) === id.type &&
            key
              .slice(sep2 + 1)
              .trim()
              .toLowerCase() === id.value.trim().toLowerCase() &&
            ownerId !== memberId
          ) {
            owner = ownerId
            break outer
          }
        }
      }

      if (!owner) throw err

      if (!attemptMerge) {
        return owner
      }

      let merged: boolean
      try {
        merged = await mergeIfAllowed(this.pgQx, this.temporal, this.log, owner, memberId)
      } catch (mergeErr) {
        // Re-throw the original constraint error, not the merge error. If we let the merge
        // error propagate, handleMemberIdentityError's checkForIdentityConstraint would
        // return false and no metadata would be stored in integration.results.error.
        // Re-throwing the constraint error lets handleMemberIdentityError retry the merge.
        this.log.warn(
          mergeErr,
          { memberId, owner },
          'merge threw during identity conflict — re-throwing constraint error for retry',
        )
        throw err
      }
      if (merged) return owner

      // noMerge, owner not found, or stale prefetch (owner === memberId):
      // Re-throw original constraint DatabaseError. handleMemberIdentityError will call
      // mergeIfAllowed again — for noMerge this is idempotent (one extra getMemberNoMerge
      // query); for merge errors it provides a natural retry.
      throw err
    }
  }

  /**
   * After an initial identity conflict redirects to a surviving member, inserts all incoming
   * identities that the surviving member doesn't already have. If a further conflict triggers
   * another merge the loop follows the new survivor. Throws ApplicationError once maxMerges
   * is reached so the error surfaces in integration.results.
   */
  private async syncIdentitiesAfterRedirect(
    initialSurvivingId: string,
    integrationId: string,
    incomingIdentities: IMemberIdentity[],
    maxMerges = 2,
  ): Promise<string> {
    let survivingId = initialSurvivingId

    for (let mergeCount = 0; ; mergeCount++) {
      const freshMap = await findIdentitiesForMembers(this.pgQx, [survivingId])
      const freshIdentities = freshMap.get(survivingId) ?? []

      // Match on (platform, type, lower(value)) only — per-member unique index excludes verified.
      // Inserting an identity that already exists with a different verified flag would hit
      // the per-member unique identity index.
      const toInsert = incomingIdentities.filter(
        (incoming) => !freshIdentities.some((existing) => isSameMemberIdentity(existing, incoming)),
      )

      // Promote verified=false → true for identities already on the surviving member
      // where the incoming payload asserts verified=true.
      const toVerify = incomingIdentities
        .filter((incoming) => incoming.verified)
        .flatMap((incoming) => {
          const existing = freshIdentities.find(
            (e) => isSameMemberIdentity(e, incoming) && !e.verified,
          )
          return existing ? [{ ...incoming, value: existing.value }] : []
        })

      if (toVerify.length > 0) {
        await this.memberRepo.updateIdentities(survivingId, toVerify)
      }

      if (toInsert.length === 0) {
        return survivingId
      }

      let nextRedirectId: string | void
      try {
        nextRedirectId = await this.insertIdentitiesWithConflictResolution(
          survivingId,
          integrationId,
          toInsert,
          true,
        )
      } catch (err) {
        // The updated member was already merged into survivingId, so handleMemberIdentityError
        // must use survivingId as the merge target instead of the absorbed member.
        throw new ApplicationError(
          'identity conflict after redirect sync',
          err instanceof Error ? err : undefined,
          { survivingId },
        )
      }

      if (!nextRedirectId) {
        return survivingId
      }

      if (mergeCount + 1 >= maxMerges) {
        throw new ApplicationError('identity sync exceeded merge limit', undefined, {
          mergeCount: mergeCount + 1,
          survivingId: nextRedirectId,
          maxMerges,
        })
      }

      survivingId = nextRedirectId
    }
  }

  public async create(
    segmentIds: string[],
    integrationId: string,
    data: IMemberCreateData,
    platform: PlatformType,
    orgPromiseCache?: OrgPromiseCache,
    activityTimestamp?: string,
  ): Promise<string> {
    return logExecutionTimeV2(
      async () => {
        try {
          this.log.debug('Creating a new member!')

          // filter empty handles and deduplicate by (platform, value, type, verified)
          data.identities = data.identities.filter(
            (identity, idx) =>
              !!identity.value &&
              data.identities.findIndex(
                (j) => isSameMemberIdentity(j, identity) && j.verified === identity.verified,
              ) === idx,
          )

          if (data.identities.length === 0) {
            throw new Error('Member must have at least one identity!')
          }

          const memberAttributeService = new MemberAttributeService(
            this.redisClient,
            this.store,
            this.log,
          )

          let attributes: Record<string, unknown> = {}
          if (data.attributes) {
            attributes = await logExecutionTimeV2(
              () => memberAttributeService.validateAttributes(platform, data.attributes),
              this.log,
              'memberService -> create -> memberAttributeService.validateAttributes',
            )

            if (attributes.location) {
              attributes.location = omitUrlValues(attributes.location)
            }

            if (!hasAttributeValue(attributes.country)) {
              const location = getAttributeValue(attributes.location)
              const country = getCountry(location)
              if (country) {
                attributes.country = Object.assign({}, attributes.country, { system: country })
              }
            }

            attributes = await logExecutionTimeV2(
              () => memberAttributeService.setAttributesDefaultValues(attributes),
              this.log,
              'memberService -> create -> memberAttributeService.setAttributesDefaultValues',
            )
          }

          data.identities = normalizeMemberIdentities(data.identities, {
            dropInvalidEmails: true,
          })

          data.displayName = normalizeDisplayName(data.displayName)

          // detect if the member is a bot
          const botDetection = this.botDetectionService.isMemberBot(
            data.identities,
            attributes,
            data.displayName,
          )

          if (botDetection === MemberBotDetection.CONFIRMED_BOT) {
            this.log.debug({ memberIdentities: data.identities }, 'Member confirmed as bot.')

            const existingIsBot = (attributes.isBot as Record<string, boolean>) || {}

            // add default and system flags only if no active flag exists
            if (!Object.values(existingIsBot).some(Boolean)) {
              attributes.isBot = { ...existingIsBot, default: true, system: true }
            }
          }

          const { id } = await logExecutionTimeV2(
            () =>
              createMember(this.pgQx, {
                displayName: data.displayName,
                joinedAt: data.joinedAt.toISOString(),
                attributes,
                reach: MemberService.calculateReach({}, data.reach),
              }),
            this.log,
            'memberService -> create -> createMember',
          )

          let redirectId: string | void
          try {
            redirectId = await logExecutionTimeV2(
              () =>
                this.insertIdentitiesWithConflictResolution(
                  id,
                  integrationId,
                  data.identities,
                  false,
                ),
              this.log,
              'memberService -> create -> insertIdentitiesWithConflictResolution',
            )
          } catch (err) {
            this.log.error(err, { memberId: id }, 'Error inserting member identities!')
            await logExecutionTimeV2(
              async () => this.memberRepo.destroyMemberAfterError(id, true),
              this.log,
              'memberService -> create -> memberRepo.destroyMemberAfterError',
            )
            throw err
          }

          let effectiveMemberId: string

          if (redirectId) {
            await this.scheduleOrphanMemberDeletion(id)
            effectiveMemberId = await this.syncIdentitiesAfterRedirect(
              redirectId,
              integrationId,
              data.identities,
            )
            await logExecutionTimeV2(
              () => this.memberRepo.addToSegments(effectiveMemberId, segmentIds),
              this.log,
              'memberService -> create -> memberRepo.addToSegments (conflict redirect)',
            )
          } else {
            effectiveMemberId = id
            try {
              await logExecutionTimeV2(
                () => this.memberRepo.addToSegments(id, segmentIds),
                this.log,
                'memberService -> create -> memberRepo.addToSegments',
              )
            } catch (err) {
              this.log.error(err, { memberId: id }, 'Error while adding member to segments!')
              await logExecutionTimeV2(
                async () => this.memberRepo.destroyMemberAfterError(id, true),
                this.log,
                'memberService -> create -> memberRepo.destroyMemberAfterError',
              )
              throw err
            }
          }

          // we should prevent organization creation for bot members
          if (botDetection === MemberBotDetection.CONFIRMED_BOT) {
            this.log.debug('Skipping organization creation for bot member')
            return effectiveMemberId
          }

          // trigger LLM validation if the member is suspected as a bot
          if (botDetection === MemberBotDetection.SUSPECTED_BOT) {
            this.log.debug(
              { memberId: effectiveMemberId },
              'Member suspected as bot. Triggering LLM validation.',
            )
            await this.startMemberBotAnalysisWithLLMWorkflow(effectiveMemberId)
          }

          const isBotMember = this.botDetectionService.isFlaggedAsBot(
            attributes as Record<string, unknown>,
          )

          await this.linkOrganizations({
            step: 'create',
            memberId: effectiveMemberId,
            segmentIds,
            integrationId,
            platform,
            data,
            orgPromiseCache,
            activityTimestamp,
            isBotMember,
          })

          return effectiveMemberId
        } catch (err) {
          this.log.error(err, 'Error while creating a new member!')
          throw err
        }
      },
      this.log,
      'memberService -> create',
    )
  }

  public async update(
    id: string,
    segmentId: string,
    integrationId: string,
    data: IMemberUpdateData,
    original: IDbMember,
    originalIdentities: IMemberIdentity[],
    platform: PlatformType,
    orgPromiseCache?: OrgPromiseCache,
    activityTimestamp?: string | ActivityTimestampsByEmailDomain,
  ): Promise<string | void> {
    return logExecutionTimeV2(
      async () => {
        this.log.trace({ memberId: id }, 'Updating a member!')

        try {
          const memberAttributeService = new MemberAttributeService(
            this.redisClient,
            this.store,
            this.log,
          )

          if (data.attributes) {
            this.log.trace({ memberId: id }, 'Validating member attributes!')
            data.attributes = await logExecutionTimeV2(
              () => memberAttributeService.validateAttributes(platform, data.attributes),
              this.log,
              'memberService -> update -> memberAttributeService.validateAttributes',
            )

            if (data.attributes.location) {
              data.attributes.location = omitUrlValues(data.attributes.location)
            }
          }

          data.identities = normalizeMemberIdentities(data.identities, {
            dropInvalidEmails: true,
          })

          // make sure displayName is proper
          if (data.displayName) {
            data.displayName = normalizeDisplayName(data.displayName)
          }

          const toUpdate = this.mergeData(original, originalIdentities, data)

          if (toUpdate.attributes) {
            this.log.trace({ memberId: id }, 'Setting attribute default values!')

            if (!hasAttributeValue(toUpdate.attributes.country)) {
              const location = getAttributeValue(toUpdate.attributes.location)
              const country = getCountry(location)
              if (country) {
                toUpdate.attributes.country = Object.assign({}, toUpdate.attributes.country, {
                  system: country,
                })
              }
            }

            toUpdate.attributes = await logExecutionTimeV2(
              () => memberAttributeService.setAttributesDefaultValues(toUpdate.attributes),
              this.log,
              'memberService -> update -> memberAttributeService.setAttributesDefaultValues',
            )
          }

          const identitiesToCreate = toUpdate.identitiesToCreate
          delete toUpdate.identitiesToCreate
          const identitiesToUpdate = toUpdate.identitiesToUpdate
          delete toUpdate.identitiesToUpdate

          if (!isObjectEmpty(toUpdate)) {
            this.log.debug({ memberId: id }, 'Updating a member!')

            const dataToUpdate = Object.entries(toUpdate).reduce((acc, [key, value]) => {
              if (key === 'identities') {
                return acc
              }

              if (value) {
                acc[key] = value
              }
              return acc
            }, {} as IDbMemberUpdateData)

            this.log.trace({ memberId: id }, 'Updating member data in db!')

            if (!isObjectEmpty(dataToUpdate)) {
              await logExecutionTimeV2(
                () => updateMember(this.pgQx, id, dataToUpdate),
                this.log,
                'memberService -> update -> updateMember',
              )
            }

            this.log.trace({ memberId: id }, 'Updating member segment association data in db!')
            await logExecutionTimeV2(
              () => this.memberRepo.addToSegments(id, [segmentId]),
              this.log,
              'memberService -> update -> memberRepo.addToSegments',
            )
          } else {
            this.log.debug({ memberId: id }, 'Nothing to update in a member!')
            await logExecutionTimeV2(
              () => this.memberRepo.addToSegments(id, [segmentId]),
              this.log,
              'memberService -> update -> memberRepo.addToSegments',
            )
          }

          let effectiveMemberId = id

          if (identitiesToCreate) {
            this.log.trace({ memberId: id }, 'Inserting new identities!')
            const redirectId = await logExecutionTimeV2(
              () =>
                this.insertIdentitiesWithConflictResolution(id, integrationId, identitiesToCreate),
              this.log,
              'memberService -> update -> insertIdentitiesWithConflictResolution',
            )
            if (redirectId) {
              effectiveMemberId = await this.syncIdentitiesAfterRedirect(
                redirectId,
                integrationId,
                identitiesToCreate,
              )
            }
          }

          if (identitiesToUpdate) {
            this.log.trace({ memberId: effectiveMemberId }, 'Updating identities!')
            try {
              await logExecutionTimeV2(
                () => this.memberRepo.updateIdentities(effectiveMemberId, identitiesToUpdate),
                this.log,
                'memberService -> update -> memberRepo.updateIdentities',
              )
            } catch (err) {
              throw new ApplicationError(
                'Error while updating member identities for an existing member!',
                err,
              )
            }
          }

          const isBotMember = this.botDetectionService.isFlaggedAsBot(
            original.attributes as Record<string, unknown>,
          )

          if (isBotMember) {
            this.log.debug({ memberId: id }, 'Skipping organization creation for bot member')
            return effectiveMemberId !== id ? effectiveMemberId : undefined
          }

          await this.linkOrganizations({
            step: 'update',
            memberId: effectiveMemberId,
            segmentIds: [segmentId],
            integrationId,
            platform,
            data,
            orgPromiseCache,
            activityTimestamp,
            isBotMember,
          })
          return effectiveMemberId !== id ? effectiveMemberId : undefined
        } catch (err) {
          this.log.error(err, { memberId: id }, 'Error while updating a member!')
          throw err
        }
      },
      this.log,
      'memberService -> update',
    )
  }

  // Without transactions, an organization taken from a cache can be merged away before the member is
  // linked to it
  private async linkOrganizations(link: IMemberOrganizationsLink): Promise<void> {
    // The retry finds what the first attempt created as existing
    const createdIds = new Set<string>()
    try {
      await this.linkOrganizationsOnce(link, false, createdIds)
    } catch (err) {
      if (!isMissingOrganizationError(err)) {
        throw err
      }
      this.log.warn(
        { memberId: link.memberId },
        'Organization to link no longer exists, looking organizations up again!',
      )
      await this.linkOrganizationsOnce(link, true, createdIds)
    }
  }

  private async linkOrganizationsOnce(
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
    const orgService = new OrganizationService(this.store, this.temporal, this.log)
    for (const org of data.organizations ?? []) {
      // Temp fix: skip the individual-noaccount.com placeholder org to avoid
      // hot-row contention on the organizations table. Permanent fix is in
      // tncTransformerBase.ts to stop emitting this org entirely.
      if (org.identities?.some((i) => i.verified && i.value === 'individual-noaccount.com')) {
        continue
      }

      this.log.trace({ memberId }, 'Finding or creating organization!')

      const result = await findOrCreateOrgOnce(
        this.orgLookups,
        orgPromiseCache,
        platform,
        org,
        (lockOrganization, organizationId) =>
          logExecutionTimeV2(
            () =>
              orgService.findOrCreate(
                platform,
                integrationId,
                org,
                lockOrganization,
                organizationId,
              ),
            this.log,
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
      this.log.trace({ memberId }, 'Assigning organization by email domain!')
      fromEmailDomain = await logExecutionTimeV2(
        () =>
          this.assignOrganizationByEmailDomain(
            integrationId,
            emailIdentities.map((i) => i.value),
            orgPromiseCache,
            memberId,
            activityTimestamp,
            isBotMember,
            refresh,
            createdIds,
          ),
        this.log,
        `memberService -> ${step} -> assignOrganizationByEmailDomain`,
      )
      organizations.push(...fromEmailDomain)
    }

    if (organizations.length === 0) {
      return
    }

    this.log.trace({ memberId }, 'Finding member organizations!')
    const orgsToAdd = (
      await Promise.all(
        uniqby(organizations, 'id').map(async (org) => {
          // Check if the org was already added to the member in the past, including deleted ones.
          // If it was, we ignore this org to prevent from adding it again.
          const existingMemberOrgs = await logExecutionTimeV2(
            () => orgService.findMemberOrganizations(memberId, org.id),
            this.log,
            `memberService -> ${step} -> orgService.findMemberOrganizations`,
          )
          return existingMemberOrgs.length > 0 ? null : org
        }),
      )
    ).filter((org) => org !== null)

    if (orgsToAdd.length === 0) {
      return
    }

    this.log.trace({ memberId }, 'Adding organizations to member!')
    try {
      await logExecutionTimeV2(
        () => orgService.addToMember(segmentIds, memberId, orgsToAdd),
        this.log,
        `memberService -> ${step} -> orgService.addToMember`,
      )
    } catch (err) {
      if (isMissingOrganizationError(err)) {
        const missingId = missingOrganizationId(err)
        await this.orgLookups.markChanged(missingId ? [missingId] : orgsToAdd.map((org) => org.id))
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

  public async assignOrganizationByEmailDomain(
    integrationId: string,
    emails: string[],
    orgPromiseCache?: OrgPromiseCache,
    memberId?: string,
    activityTimestamp?: string | ActivityTimestampsByEmailDomain,
    isBotMember = false,
    refresh = false,
    createdIds?: Set<string>,
  ): Promise<EmailDomainOrganization[]> {
    const orgService = new OrganizationService(this.store, this.temporal, this.log)
    const organizations: EmailDomainOrganization[] = []
    const emailDomains = new Set<string>()

    // Collect unique domains
    for (const email of emails) {
      if (email) {
        const domain = email.split('@')[1]
        // domain can be undefined if email is invalid
        if (domain) {
          if (!isDomainExcluded(domain)) {
            emailDomains.add(domain)
          }
        }
      }
    }

    // Assign member to organization based on email domain
    for (const domain of emailDomains) {
      const orgSource = OrganizationSource.EMAIL_DOMAIN
      const org: IOrganization = {
        attributes: {
          name: {
            integration: [domain],
          },
        },
        identities: [
          {
            value: domain,
            type: OrganizationIdentityType.PRIMARY_DOMAIN,
            platform: 'email',
            verified: true,
            source: orgSource,
          },
        ],
      }
      const result = await findOrCreateOrgOnce(
        this.orgLookups,
        orgPromiseCache,
        OrganizationAttributeSource.EMAIL,
        org,
        (lockOrganization, organizationId) =>
          orgService.findOrCreate(
            OrganizationAttributeSource.EMAIL,
            integrationId,
            org,
            lockOrganization,
            organizationId,
          ),
        refresh,
      )
      if (result?.created) {
        createdIds?.add(result.id)
      }
      if (result) {
        organizations.push({
          id: result.id,
          source: orgSource,
          created: result.created,
        })

        if (memberId && activityTimestamp && !isBotMember) {
          await this.bufferMemberOrganizationActivityDates(
            memberId,
            result.id,
            typeof activityTimestamp === 'string'
              ? [activityTimestamp]
              : (activityTimestamp.get(domain) ?? []),
          )
        }
      }
    }

    return organizations
  }

  public async processMemberUpdate(
    integrationId: string,
    platform: PlatformType,
    member: IMemberData,
  ): Promise<void> {
    this.log = getChildLogger('MemberService.processMemberUpdate', this.log, {
      integrationId,
    })

    try {
      this.log.debug('Processing member update.')

      if (!member.identities || member.identities.length === 0) {
        const errorMessage = `Member can't be updated. It is missing identities fields.`
        this.log.warn(errorMessage)
        return
      }

      const integrationRepo = new IntegrationRepository(this.store, this.log)

      const dbIntegration = await integrationRepo.findById(integrationId)
      const segmentId = dbIntegration.segmentId

      // first try finding the member using the identity
      const identity = singleOrDefault(
        member.identities,
        (i) => i.platform === platform && i.type === MemberIdentityType.USERNAME,
      )
      const dbMembers = await findMembersByVerifiedUsernames(this.pgQx, [
        {
          segmentId,
          platform,
          username: identity.value,
        },
      ])

      const dbMember = dbMembers.size === 0 ? undefined : Array.from(dbMembers.values())[0]

      if (dbMember) {
        this.log.trace({ memberId: dbMember.id }, 'Found existing member.')

        this.log.trace({ memberId: dbMember.id }, 'Fetching member identities!')
        const dbIdentities = await logExecutionTimeV2(
          async () => {
            const identities = await findIdentitiesForMembers(this.pgQx, [dbMember.id])
            return identities.get(dbMember.id)
          },
          this.log,
          'memberService -> processMemberUpdate -> findIdentitiesForMembers',
        )

        await this.update(
          dbMember.id,
          segmentId,
          integrationId,
          {
            attributes: member.attributes,
            joinedAt: member.joinedAt ? new Date(member.joinedAt) : undefined,
            identities: member.identities,
            organizations: member.organizations,
            displayName: member.displayName || undefined,
            reach: member.reach || undefined,
          },
          dbMember,
          dbIdentities,
          platform,
        )
      } else {
        this.log.debug('No member found for updating. This member update process had no affect.')
      }
    } catch (err) {
      this.log.error(err, 'Error while processing a member update!')
      throw err
    }
  }

  private mergeData(
    dbMember: IDbMember,
    dbIdentities: IMemberIdentity[],
    member: IMemberUpdateData,
  ): IDbMemberUpdateData {
    let joinedAt: string | undefined
    if (member.joinedAt) {
      const newDate = member.joinedAt
      const oldDate = new Date(dbMember.joinedAt)

      joinedAt = getEarliestMemberJoinedAt(oldDate, new Date(newDate)).toISOString()

      if (joinedAt === oldDate.toISOString()) {
        joinedAt = undefined
      }
    }

    let identitiesToCreate: IMemberIdentity[] | undefined
    let identitiesToUpdate: IMemberIdentity[] | undefined
    if (member.identities && member.identities.length > 0) {
      const newIdentities: IMemberIdentity[] = []
      const toUpdate: IMemberIdentity[] = []
      for (const identity of member.identities) {
        const dbIdentity = dbIdentities.find((t) => isSameMemberIdentity(t, identity))
        if (!dbIdentity) {
          newIdentities.push(identity)
        } else if (dbIdentity.verified !== identity.verified) {
          // Keep stored preferred casing; updateIdentities matches on exact value.
          toUpdate.push({ ...identity, value: dbIdentity.value })
        }
      }

      if (newIdentities.length > 0) {
        identitiesToCreate = newIdentities
      }

      if (toUpdate.length > 0) {
        identitiesToUpdate = toUpdate
      }
    }

    let attributes: Record<string, unknown> | undefined
    if (member.attributes) {
      const incomingIsBot = member.attributes?.[MemberAttributeName.IS_BOT]?.[PlatformType.GITHUB]
      const existingIsBot = dbMember.attributes?.[MemberAttributeName.IS_BOT]?.[PlatformType.GITHUB]

      // Incoming data flags the member as a bot, but the existing record does not
      // This is likely corrupted; discard the incoming attributes
      // If the member were actually a bot, the flag would have been set at creation.
      if (incomingIsBot && !existingIsBot) {
        this.log.warn(
          { memberId: dbMember.id },
          'Member attributes appear corrupted due to bot attributes',
        )

        member.attributes = {} // Preserve existing member attributes
      }

      const temp = mergeWith({}, dbMember.attributes, member.attributes)
      const manuallyChangedFields: string[] = dbMember.manuallyChangedFields || []

      if (manuallyChangedFields.length > 0) {
        const prefix = 'attributes.'

        const manuallyChangedAttributes = [
          ...new Set(
            manuallyChangedFields
              .filter((f) => f.startsWith(prefix))
              .map((f) => f.slice(prefix.length)),
          ),
        ]

        // Preserve manually changed attributes
        for (const key of manuallyChangedAttributes) {
          if (dbMember.attributes?.[key] !== undefined) {
            temp[key] = dbMember.attributes[key]
          }
        }
      }

      if (!isEqual(temp, dbMember.attributes)) {
        attributes = temp
      }
    }

    let reach: Partial<Record<PlatformType, number>> | undefined
    if (member.reach) {
      const temp = MemberService.calculateReach(dbMember.reach, member.reach)
      if (!isEqual(temp, dbMember.reach)) {
        reach = temp
      }
    }

    return {
      joinedAt,
      attributes,
      identitiesToCreate,
      identitiesToUpdate,
      // we don't want to update the display name if it's already set
      // returned value should be undefined here otherwise it will cause an update!
      displayName: dbMember.displayName ? undefined : member.displayName,
      reach,
    }
  }

  private static calculateReach(
    oldReach: Partial<Record<PlatformType | 'total', number>>,
    newReach: Partial<Record<PlatformType, number>>,
  ) {
    // Totals are recomputed, so we delete them first
    delete oldReach.total
    const out = mergeWith({}, oldReach, newReach)
    if (Object.keys(out).length === 0) {
      return { total: -1 }
    }
    // Total is the sum of all attributes
    out.total = Object.values(out).reduce((a: number, b: number) => a + b, 0)
    return out
  }

  private async scheduleOrphanMemberDeletion(memberId: string): Promise<void> {
    try {
      await this.temporal.workflow.start('deleteOrphanMember', {
        taskQueue: 'entity-merging',
        workflowId: `${TemporalWorkflowId.DELETE_ORPHAN_MEMBER}/${memberId}`,
        args: [memberId],
      })
    } catch (err) {
      this.log.error(err, { memberId }, 'Failed to schedule orphan member deletion!')
    }
  }

  private async startMemberBotAnalysisWithLLMWorkflow(memberId: string): Promise<void> {
    await this.temporal.workflow.start('processMemberBotAnalysisWithLLM', {
      taskQueue: 'profiles',
      workflowId: `${TemporalWorkflowId.MEMBER_BOT_ANALYSIS_WITH_LLM}/${memberId}`,
      retry: {
        maximumAttempts: 10,
      },
      args: [{ memberId }],
      searchAttributes: {
        TenantId: [DEFAULT_TENANT_ID],
      },
    })
  }

  /**
   * Queues a member activity date in Redis for stint inference.
   * Uses SADD for natural concurrency safety and deduplication.
   */
  private async bufferMemberOrganizationActivityDates(
    memberId: string,
    organizationId: string,
    activityTimestamps: string[],
  ): Promise<void> {
    const dates = Array.from(
      new Set(activityTimestamps.map((t) => new Date(t).toISOString().split('T')[0])),
    )
    if (dates.length === 0) {
      return
    }

    // Each member gets one flat set: values are "orgId|date"
    const datesKey = `${MEMBER_ORG_STINT_CHANGES_DATES_PREFIX}:${memberId}`
    const values = dates.map((date) => `${organizationId}|${date}`)

    await this.redisClient
      .multi()
      .sAdd(datesKey, values)
      .sAdd(MEMBER_ORG_STINT_CHANGES_QUEUE, memberId)
      .exec()

    this.log.debug(
      { memberId, organizationId, dates },
      'Buffered activity dates and queued member.',
    )
  }
}
