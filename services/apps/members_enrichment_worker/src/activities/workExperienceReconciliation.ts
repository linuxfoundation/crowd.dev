import { generateUUIDv1 } from '@crowd/common'
import { IMemberOrganizationData, OrganizationSource } from '@crowd/types'

import { IMemberEnrichmentDataNormalizedOrganization } from '../types'

export interface IWorkExperienceChanges {
  toDelete: IMemberOrganizationData[]
  toCreate: IMemberEnrichmentDataNormalizedOrganization[]
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  toUpdate: Map<IMemberOrganizationData, Record<string, any>>
}

const normalizeTitle = (title: string | null | undefined) => (title ?? '').trim().toLowerCase()
const normalizeDate = (date: string | null | undefined) => (date ? date.substring(0, 10) : '')
const dateTupleKey = (
  orgId: string,
  start: string | null | undefined,
  end: string | null | undefined,
) => `${orgId}|${normalizeDate(start)}|${normalizeDate(end)}`

/**
 * Returns true when the set of (orgId, startDate, endDate) tuples differs
 * between deletes and creates. Fields like title or source don't affect
 * the affiliation timeline, so they're intentionally ignored.
 */
export function hasMemberOrganizationTimelineChange(
  toDelete: IMemberOrganizationData[],
  toCreate: IMemberEnrichmentDataNormalizedOrganization[],
): boolean {
  const deletedKeys = new Set(toDelete.map((d) => dateTupleKey(d.orgId, d.dateStart, d.dateEnd)))
  const createdKeys = new Set(
    toCreate.map((c) => dateTupleKey(c.organizationId, c.startDate, c.endDate)),
  )

  if (deletedKeys.size !== createdKeys.size) return true
  for (const key of deletedKeys) {
    if (!createdKeys.has(key)) return true
  }
  return false
}

interface IPendingOrgUpdate {
  oldRow: IMemberOrganizationData
  entry: IMemberEnrichmentDataNormalizedOrganization
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  toUpdateInner: Record<string, any>
}

/**
 * The unique index on (memberId, organizationId, dateStart, dateEnd) means an in-place update
 * can only be applied while its target tuple isn't still held by another row. Schedules
 * date-changing updates in an order where every target tuple is free by the time it runs;
 * rows caught in a genuine swap/cycle (no valid order exists) fall back to delete+create,
 * which the caller always applies before any update.
 */
function scheduleDateChangingUpdates(
  pending: IPendingOrgUpdate[],
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  toUpdate: Map<IMemberOrganizationData, Record<string, any>>,
  toCreate: IMemberEnrichmentDataNormalizedOrganization[],
  toDelete: IMemberOrganizationData[],
) {
  const targetKey = (u: IPendingOrgUpdate) =>
    dateTupleKey(
      u.oldRow.orgId,
      u.toUpdateInner.dateStart ?? u.oldRow.dateStart,
      u.toUpdateInner.dateEnd ?? u.oldRow.dateEnd,
    )
  const currentKey = (u: IPendingOrgUpdate) =>
    dateTupleKey(u.oldRow.orgId, u.oldRow.dateStart, u.oldRow.dateEnd)

  let remaining = pending
  let progress = true
  while (remaining.length > 0 && progress) {
    progress = false
    const stillHeldKeys = new Set(remaining.map(currentKey))
    const next: IPendingOrgUpdate[] = []
    for (const u of remaining) {
      if (!stillHeldKeys.has(targetKey(u))) {
        toUpdate.set(u.oldRow, u.toUpdateInner)
        progress = true
      } else {
        next.push(u)
      }
    }
    remaining = next
  }

  // a genuine cycle — no sequential order frees every target tuple in time
  for (const u of remaining) {
    toDelete.push(u.oldRow)
    toCreate.push(u.entry)
  }
}

/**
 * Reconciles enrichment-owned memberOrganizations rows against the incoming payload
 * in place: matched rows are updated (only the fields that actually changed), unmatched
 * old rows are deleted, unmatched new entries are created. Never touches UI/project-registry
 * or verified rows — callers must exclude those from oldEnrichmentRows and filter matching
 * newEntries out beforehand.
 */
function reconcileEnrichmentOrgs(
  oldEnrichmentRows: IMemberOrganizationData[],
  newEntries: IMemberEnrichmentDataNormalizedOrganization[],
): IWorkExperienceChanges {
  const oldByOrg = new Map<string, IMemberOrganizationData[]>()
  for (const old of oldEnrichmentRows) {
    const bucket = oldByOrg.get(old.orgId) ?? []
    bucket.push(old)
    oldByOrg.set(old.orgId, bucket)
  }

  const toCreate: IMemberEnrichmentDataNormalizedOrganization[] = []
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const toUpdate: Map<IMemberOrganizationData, Record<string, any>> = new Map()
  const matchedOldIds = new Set<string>()
  const pendingDateChanges: IPendingOrgUpdate[] = []

  for (const entry of newEntries) {
    const candidates = (oldByOrg.get(entry.organizationId) ?? []).filter(
      (c) => !matchedOldIds.has(c.id),
    )
    const match =
      candidates.find(
        (c) =>
          normalizeTitle(c.jobTitle) === normalizeTitle(entry.title) &&
          normalizeDate(c.dateStart) === normalizeDate(entry.startDate) &&
          normalizeDate(c.dateEnd) === normalizeDate(entry.endDate),
      ) ??
      candidates.find((c) => normalizeTitle(c.jobTitle) === normalizeTitle(entry.title)) ??
      candidates[0]

    if (!match) {
      toCreate.push(entry)
      continue
    }

    matchedOldIds.add(match.id)

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const toUpdateInner: Record<string, any> = {}
    if (entry.title !== undefined && entry.title !== match.jobTitle) {
      toUpdateInner.title = entry.title
    }
    const startChanged = normalizeDate(entry.startDate) !== normalizeDate(match.dateStart)
    const endChanged = normalizeDate(entry.endDate) !== normalizeDate(match.dateEnd)
    if (startChanged) {
      toUpdateInner.dateStart = entry.startDate
    }
    if (endChanged) {
      toUpdateInner.dateEnd = entry.endDate
    }
    if (entry.source !== undefined && entry.source !== match.source) {
      toUpdateInner.source = entry.source
    }
    if (Object.keys(toUpdateInner).length === 0) {
      continue
    }
    if (startChanged || endChanged) {
      pendingDateChanges.push({ oldRow: match, entry, toUpdateInner })
    } else {
      toUpdate.set(match, toUpdateInner)
    }
  }

  const toDelete = oldEnrichmentRows.filter((old) => !matchedOldIds.has(old.id))

  scheduleDateChangingUpdates(pendingDateChanges, toUpdate, toCreate, toDelete)

  return { toDelete, toCreate, toUpdate }
}

export function prepareWorkExperiences(
  oldVersion: IMemberOrganizationData[],
  newVersion: IMemberEnrichmentDataNormalizedOrganization[],
  isHighConfidenceSourceSelectedForWorkExperiences: boolean,
  deletedOrganizationIds: Set<string>,
): IWorkExperienceChanges {
  // UI and project-registry rows are manual input; a verified row is a human decision too —
  // the worker never deletes or updates any of them.
  const oldEnrichmentRows = oldVersion.filter(
    (c) =>
      c.source !== OrganizationSource.UI &&
      c.source !== OrganizationSource.PROJECT_REGISTRY &&
      c.verified !== true,
  )

  // never recreate an affiliation that a person deleted on purpose — providers keep resupplying it
  newVersion = newVersion.filter((e) => !deletedOrganizationIds.has(e.organizationId))

  // verified rows are excluded from oldEnrichmentRows above, so a matching provider entry
  // must be dropped here too, or it lands in toCreate as a conflicting duplicate. Match on
  // e.organizationId, not e.identities — enrichment resolves the org onto organizationId
  // without necessarily adding it to identities.
  const verifiedRows = oldVersion.filter((c) => c.verified === true)
  newVersion = newVersion.filter(
    (e) =>
      !verifiedRows.some(
        (v) =>
          normalizeTitle(v.jobTitle) === normalizeTitle(e.title) && e.organizationId === v.orgId,
      ),
  )

  if (isHighConfidenceSourceSelectedForWorkExperiences) {
    const uiEntries = oldVersion.filter((c) => c.source === OrganizationSource.UI)
    const filteredNewVersion = newVersion.filter(
      (e) => !uiEntries.some((ui) => e.title === ui.jobTitle && e.organizationId === ui.orgId),
    )
    return reconcileEnrichmentOrgs(oldEnrichmentRows, filteredNewVersion)
  }

  // sort both versions by start date and only use manual changes from the current version
  const orderedCurrentVersion = oldVersion
    .filter((c) => c.source === OrganizationSource.UI)
    .sort((a, b) => {
      // If either value is null/undefined, move it to the beginning
      if (!a.dateStart && !b.dateStart) return 0
      if (!a.dateStart) return -1
      if (!b.dateStart) return 1

      // Compare dates if both values exist
      return new Date(a.dateStart as string).getTime() - new Date(b.dateStart as string).getTime()
    })

  let orderedNewVersion = newVersion.sort((a, b) => {
    // If either value is null/undefined, move it to the beginning
    if (!a.startDate && !b.startDate) return 0
    if (!a.startDate) return -1
    if (!b.startDate) return 1

    // Compare dates if both values exist
    return new Date(a.startDate as string).getTime() - new Date(b.startDate as string).getTime()
  })

  // set ids and new flag to new versions just so we can easily manipulate the array later
  for (const exp of orderedNewVersion) {
    exp.id = generateUUIDv1()
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const uiDateEndFills: Map<IMemberOrganizationData, Record<string, any>> = new Map()

  // we iterate through the existing version experiences to see if update is needed
  for (const current of orderedCurrentVersion) {
    // try and find a matching experience in the new versions by title
    const match = orderedNewVersion.find(
      (e) => e.title === current.jobTitle && e.organizationId === current.orgId,
    )

    // if we found a match we can check if we need something to update
    if (
      match &&
      current.dateStart === match.startDate &&
      current.dateEnd === null &&
      match.endDate !== null
    ) {
      uiDateEndFills.set(current, { dateEnd: match.endDate })

      // remove the match from the new version array so we later don't process it again
      orderedNewVersion = orderedNewVersion.filter((e) => e.id !== match.id)
    } else if (
      match &&
      (current.dateStart !== match.startDate || current.dateEnd !== null || match.endDate === null)
    ) {
      // there's an incoming work experiences, but it's conflicting with the existing manually updated data
      // we shouldn't add or update anything when this happens
      // we can only update dateEnd of existing manually changed data, when it has a null dateEnd
      orderedNewVersion = orderedNewVersion.filter((e) => e.id !== match.id)
    }
    // if we didn't find a match we should just leave it as it is in the database since it was manual input
  }

  const results = reconcileEnrichmentOrgs(oldEnrichmentRows, orderedNewVersion)
  for (const [current, toUpdateInner] of uiDateEndFills) {
    results.toUpdate.set(current, toUpdateInner)
  }

  return results
}
