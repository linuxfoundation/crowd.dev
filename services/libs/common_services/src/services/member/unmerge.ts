import { randomUUID } from 'crypto'
import pick from 'lodash.pick'
import uniqBy from 'lodash.uniqby'

import { BadRequestError, DEFAULT_TENANT_ID, getProperDisplayName } from '@crowd/common'
import {
  MEMBER_MERGE_FIELDS,
  MemberField,
  QueryExecutor,
  addMemberRole,
  createMember,
  createMemberIdentity,
  deleteManyMemberIdentities,
  fetchManyMemberOrgsWithOrgData,
  fetchMemberIdentities,
  findAlreadyExistingVerifiedIdentities,
  findMemberById,
  findNonExistingOrganizationIds,
  removeMemberRole,
  updateMember,
} from '@crowd/data-access-layer'
import { addMemberNoMerge } from '@crowd/data-access-layer/src/member_merge'
import {
  findMemberAffiliations,
  moveSelectedAffiliationsBetweenMembers,
} from '@crowd/data-access-layer/src/member_segment_affiliations'
import { MemberUpdateInput } from '@crowd/data-access-layer/src/members/base'
import {
  findMemberIdentitiesByValue,
  findMemberIdentityById,
} from '@crowd/data-access-layer/src/members/identities'
import {
  addMergeAction,
  findMergeBackup,
  setMergeAction,
} from '@crowd/data-access-layer/src/mergeActions/repo'
import { getServiceLogger } from '@crowd/logging'
import { Client as TemporalClient } from '@crowd/temporal'
import {
  IAttributes,
  IMemberAffiliationMergeBackup,
  IMemberContribution,
  IMemberIdentity,
  IMemberRenderFriendlyRole,
  IMemberRoleWithOrganization,
  IMemberUnmergeBackup,
  IMemberUnmergePreviewResult,
  IMemberUsername,
  IUnmergePreviewResult,
  MemberBotDetection,
  MemberIdentityType,
  MemberRoleUnmergeStrategy,
  MemberRow,
  MemberUnmergeResult,
  MergeActionState,
  MergeActionStep,
  MergeActionType,
} from '@crowd/types'

import { BotDetectionService } from '../bot.service'
import { unmergeRoles } from '../memberOrganization'

const logger = getServiceLogger()

/**
 * Revert only attributes that came through the merge, leaving other attributes intact.
 *
 * 1) Both primary and secondary backup have the attribute → remove platform keys that came from secondary
 * 2) Only secondary backup has the attribute → remove platform keys matching secondary's values
 * 3) Only primary backup has the attribute → keep current value (nothing to revert)
 * 4) Neither backup has the attribute → keep current value
 *
 * We only act on cases 1 and 2.
 */
function revertAttributes(
  current: IAttributes,
  primaryBackup: IAttributes,
  secondaryBackup: IAttributes,
  manualFields: string[],
): IAttributes {
  const result: IAttributes = {}

  for (const attributeKey of Object.keys(current)) {
    if (manualFields.some((f) => f === `attributes.${attributeKey}`)) {
      result[attributeKey] = { ...current[attributeKey] }
      continue
    }

    const pAttr = primaryBackup[attributeKey]
    const sAttr = secondaryBackup[attributeKey]

    // both backups have the attribute
    if (pAttr && sAttr) {
      const cleaned = { ...current[attributeKey] }
      // find platform key values that exist on secondary, but not on primary backup
      for (const platformKey of Object.keys(sAttr)) {
        if (pAttr[platformKey] == null || pAttr[platformKey] === '') {
          // check current member still has this value for the attribute[key][platform], and primary backup didn't have this value
          if (
            cleaned[platformKey] === sAttr[platformKey] &&
            pAttr[platformKey] !== cleaned[platformKey]
          ) {
            delete cleaned[platformKey]
          }
        }
      }
      // check any platform keys remaining on current member, if not remove the attribute completely
      if (Object.keys(cleaned).length > 0) {
        result[attributeKey] = cleaned
      }
    } else if (!pAttr && sAttr && current[attributeKey]) {
      // remove platform keys that have the same value with current member
      const cleaned = { ...current[attributeKey] }
      for (const platformKey of Object.keys(cleaned)) {
        if (cleaned[platformKey] === sAttr[platformKey]) {
          delete cleaned[platformKey]
        }
      }
      // check any platform keys remaining on current member, if not remove the attribute completely
      if (Object.keys(cleaned).length > 0) {
        result[attributeKey] = cleaned
      }
    } else {
      result[attributeKey] = { ...current[attributeKey] }
    }
  }

  return result
}

// only act on reach if current member has some data
function revertReach(
  current: Record<string, number>,
  secondaryBackup: Record<string, number>,
): Record<string, number> {
  const result: Record<string, number> = {}
  for (const [key, value] of Object.entries(current)) {
    if (key !== 'total' && secondaryBackup[key] === value) continue
    if (key !== 'total') result[key] = value
  }
  // check if there are any keys other than total, if yes recalculate total, else set total to -1
  const values = Object.values(result)
  result.total = values.length > 0 ? values.reduce((a, b) => a + b, 0) : -1
  return result
}

function getUsernameFromIdentities(identities: IMemberIdentity[]): IMemberUsername {
  const username: IMemberUsername = {}
  for (const i of identities) {
    if (i.type !== MemberIdentityType.USERNAME) continue
    if (username[i.platform]) {
      ;(username[i.platform] as string[]).push(i.value)
    } else {
      username[i.platform] = [i.value]
    }
  }
  return username
}

export function calculateRenderFriendlyOrganizations(
  roles: IMemberRoleWithOrganization[],
): IMemberRenderFriendlyRole[] {
  return roles.map((role) => ({
    id: role.organizationId,
    displayName: role.organizationName,
    logo: role.organizationLogo,
    memberOrganizations: role,
  }))
}

/**
 * Build a preview of the unmerge outcome without side effects.
 *
 * When `revertPreviousMerge` is true, restores both members from stored
 * merge backups. Otherwise extracts the selected identity into a new
 * member. Manual fields are always preserved.
 */
export async function prepareMemberUnmerge(
  qx: QueryExecutor,
  memberId: string,
  identityId: string,
  revertPreviousMerge = false,
): Promise<IUnmergePreviewResult<IMemberUnmergePreviewResult>> {
  const memberById = await findMemberById(qx, memberId, [
    MemberField.ID,
    MemberField.DISPLAY_NAME,
    MemberField.ATTRIBUTES,
    MemberField.REACH,
    MemberField.CONTRIBUTIONS,
    MemberField.MANUALLY_CHANGED_FIELDS,
  ])

  if (!memberById) {
    throw new BadRequestError(`Member ${memberId} not found`)
  }

  logger.info({ memberId }, 'Fetching member relations (identities, affiliations, organizations)')

  const [memberOrganizationsMap, identities, affiliations] = await Promise.all([
    fetchManyMemberOrgsWithOrgData(qx, [memberId]),
    fetchMemberIdentities(qx, memberId),
    findMemberAffiliations(qx, memberId),
  ])

  logger.info({ memberId }, 'Done fetching member relations')

  const memberOrganizations = memberOrganizationsMap.get(memberId) ?? []

  const member = {
    ...memberById,
    memberOrganizations,
    identities,
    affiliations,
  }

  const identity = await findMemberIdentityById(qx, memberId, identityId)

  if (!identity) {
    throw new BadRequestError('Identity not found or does not belong to this member')
  }

  if (revertPreviousMerge) {
    logger.info({ memberId }, 'Finding merge backup for revert')

    const mergeAction = await findMergeBackup(qx, memberId, MergeActionType.MEMBER, identity)

    if (!mergeAction) {
      throw new BadRequestError('No previous merge action found to revert for member!')
    }

    logger.info({ memberId }, 'Merge backup found, generating unmerge preview')
    const primaryBackup = mergeAction.unmergeBackup.primary as IMemberUnmergeBackup
    const secondaryBackup = mergeAction.unmergeBackup.secondary as IMemberUnmergeBackup

    const remainingIdentitiesInCurrentMember = member.identities.filter(
      (i: IMemberIdentity) =>
        !secondaryBackup.identities.some(
          (s) => s.platform === i.platform && s.value === i.value && s.type === i.type,
        ),
    )

    // Only unmerge when primary member still has some identities left after removing identities in the secondary backup
    // if not fall back to identity extraction
    if (remainingIdentitiesInCurrentMember.length > 0) {
      // construct primary member with best effort
      const manualFields = member.manuallyChangedFields ?? []

      const revertedAttributes = manualFields.includes('attributes')
        ? member.attributes
        : revertAttributes(
            member.attributes ?? {},
            primaryBackup.attributes ?? {},
            secondaryBackup.attributes ?? {},
            manualFields,
          )

      const revertedReach = manualFields.includes('reach')
        ? member.reach
        : revertReach(member.reach ?? { total: -1 }, secondaryBackup.reach ?? {})

      // check secondary member has any contributions to extract from current member
      const secondaryContributions = Array.isArray(secondaryBackup.contributions)
        ? secondaryBackup.contributions
        : []

      const revertedContributions =
        manualFields.includes('contributions') || !Array.isArray(member.contributions)
          ? member.contributions
          : member.contributions.filter((c) => !secondaryContributions.some((s) => s.id === c.id))

      // If displayName came from the secondary member through merge, revert it
      const revertedDisplayName =
        !manualFields.includes('displayName') &&
        primaryBackup.displayName !== member.displayName &&
        secondaryBackup.displayName === member.displayName
          ? null
          : member.displayName

      // identities: Remove identities coming from secondary backup
      const revertedIdentities = member.identities.filter(
        (i: IMemberIdentity) =>
          !secondaryBackup.identities.some(
            (s) => s.platform === i.platform && s.value === i.value && s.type === i.type,
          ),
      )

      // affiliations: Remove affiliations coming from secondary backup
      const revertedAffiliations = member.affiliations.filter(
        (a) => !secondaryBackup.affiliations.some((s) => s.id === a.id),
      )

      // member organizations
      const unmergedRoles = unmergeRoles(
        member.memberOrganizations,
        primaryBackup.memberOrganizations,
        secondaryBackup.memberOrganizations,
        MemberRoleUnmergeStrategy.SAME_MEMBER,
      ) as IMemberRoleWithOrganization[]

      return {
        primary: {
          ...pick(member, MEMBER_MERGE_FIELDS),
          displayName: revertedDisplayName,
          attributes: revertedAttributes,
          reach: revertedReach,
          contributions: revertedContributions,
          identities: revertedIdentities,
          memberOrganizations: unmergedRoles,
          affiliations: revertedAffiliations,
          organizations: calculateRenderFriendlyOrganizations(unmergedRoles),
          username: getUsernameFromIdentities(revertedIdentities),
          numberOfOpenSourceContributions: revertedContributions?.length ?? 0,
        },
        secondary: {
          ...secondaryBackup,
          organizations: calculateRenderFriendlyOrganizations(secondaryBackup.memberOrganizations),
          numberOfOpenSourceContributions: secondaryBackup.contributions?.length ?? 0,
        },
      }
    }
  }

  // Identity extraction preview will be generated if revertMerge flag is not set
  let secondaryIdentities: IMemberIdentity[] = [identity]

  // For email identities, extract all related identities across sources
  if (identity.type === MemberIdentityType.EMAIL) {
    const allEmailIdentities = await findMemberIdentitiesByValue(qx, memberId, identity.value, {
      type: MemberIdentityType.EMAIL,
    })

    secondaryIdentities = uniqBy([...allEmailIdentities, identity], (i) => i.id)
  }

  // Ensure primary member retains at least one identity
  const primaryIdentities = member.identities.filter(
    (i: IMemberIdentity) =>
      !secondaryIdentities.some(
        (s) => s.platform === i.platform && s.value === i.value && s.type === i.type,
      ),
  )

  if (primaryIdentities.length === 0) {
    throw new BadRequestError('Cannot unmerge: primary member must retain at least one identity')
  }

  const secondaryDisplayName = getProperDisplayName(identity.value)
  const secondaryAttributes: IAttributes = {}

  const botDetection = new BotDetectionService(logger).isMemberBot(
    secondaryIdentities,
    secondaryAttributes,
    secondaryDisplayName,
  )

  if (botDetection === MemberBotDetection.CONFIRMED_BOT) {
    secondaryAttributes.isBot = { default: true, system: true }
  }

  return {
    primary: {
      ...pick(member, MEMBER_MERGE_FIELDS),
      identities: primaryIdentities,
      memberOrganizations,
      organizations: calculateRenderFriendlyOrganizations(memberOrganizations),
      username: getUsernameFromIdentities(primaryIdentities),
      numberOfOpenSourceContributions: member.contributions?.length ?? 0,
    },
    secondary: {
      id: randomUUID(),
      reach: { total: -1 },
      username: getUsernameFromIdentities(secondaryIdentities),
      displayName: secondaryDisplayName,
      identities: secondaryIdentities,
      memberOrganizations: [],
      organizations: [],
      attributes: secondaryAttributes,
      joinedAt: new Date().toISOString(),
      affiliations: [],
      contributions: [],
      manuallyCreated: true,
      manuallyChangedFields: [],
      numberOfOpenSourceContributions: 0,
    },
  }
}

function sameRole(a: IMemberRoleWithOrganization, b: IMemberRoleWithOrganization) {
  return a.organizationId === b.organizationId && a.title === b.title && a.dateStart === b.dateStart
}

function toPreviewMember(
  row: MemberRow,
  identities: IMemberIdentity[],
  affiliations: IMemberAffiliationMergeBackup[],
  memberOrganizations: IMemberRoleWithOrganization[],
  contributions: IMemberContribution[] | undefined,
): IMemberUnmergePreviewResult {
  return {
    ...row,
    identities,
    affiliations,
    memberOrganizations,
    username: getUsernameFromIdentities(identities),
    organizations: calculateRenderFriendlyOrganizations(memberOrganizations),
    numberOfOpenSourceContributions: contributions?.length ?? 0,
  }
}

/**
 * Execute an unmerge transactionally.
 *
 * Removes identities from primary, creates the secondary member,
 * moves affiliations and org roles, updates primary fields.
 * Records the action and prevents re-merge suggestions.
 */
export async function unmergeMember(
  tx: QueryExecutor,
  memberId: string,
  payload: IUnmergePreviewResult<IMemberUnmergePreviewResult>,
  actorId?: string,
): Promise<MemberUnmergeResult> {
  const member = await findMemberById(tx, memberId, [MemberField.ID])

  if (!member) {
    throw new BadRequestError(`Member ${memberId} not found`)
  }

  const { primary, secondary } = payload

  // Remove identities in secondary member from primary member
  // Keep unverified identities on primary only if primary also has a matching unverified identity
  const identitiesToRemove = secondary.identities.filter(
    (i) =>
      i.verified === undefined || // backwards compatibility for old identity backups
      i.verified === true ||
      (i.verified === false &&
        !primary.identities.some(
          (pi) =>
            pi.verified === false &&
            pi.platform === i.platform &&
            pi.value === i.value &&
            pi.type === i.type,
        )),
  )

  if (identitiesToRemove.length > 0) {
    await deleteManyMemberIdentities(tx, {
      memberId,
      identities: identitiesToRemove.map((i) => ({
        platform: i.platform,
        value: i.value,
        type: i.type,
      })),
    })
  }

  // Exclude identities in secondary that already exist on another member
  const identitiesToExclude = await findAlreadyExistingVerifiedIdentities(tx, {
    identities: secondary.identities.filter((i) => i.verified),
  })

  const secondaryIdentities = secondary.identities.filter(
    (i) =>
      !identitiesToExclude.some(
        (ie) =>
          ie.platform === i.platform && ie.value === i.value && ie.type === i.type && ie.verified,
      ),
  )

  // Track organizations that don't exist (for filtering secondary orgs)
  let nonExistingOrgIds: string[] = []
  // Track roles deleted from primary (for filtering primary orgs)
  let rolesToDelete: IMemberRoleWithOrganization[] = []

  // Create the secondary member
  const secondaryRow = await createMember(tx, {
    id: secondary.id,
    displayName: secondary.displayName,
    joinedAt: secondary.joinedAt,
    attributes: secondary.attributes,
    reach: secondary.reach,
    manuallyCreated: secondary.manuallyCreated,
    contributions: secondary.contributions,
  })

  const secondaryId = secondaryRow.id

  // Track merge action
  await addMergeAction(
    tx,
    MergeActionType.MEMBER,
    memberId,
    secondaryId,
    MergeActionStep.UNMERGE_STARTED,
    MergeActionState.IN_PROGRESS,
    undefined,
    actorId,
  )

  // Create identities for the secondary member
  for (const i of secondaryIdentities) {
    await createMemberIdentity(tx, {
      memberId: secondaryId,
      platform: i.platform,
      type: i.type,
      value: i.value,
      sourceId: i.sourceId || null,
      integrationId: i.integrationId || null,
      verified: i.verified,
      source: i.source,
    })
  }

  // Move affiliations
  if (secondary.affiliations.length > 0) {
    await moveSelectedAffiliationsBetweenMembers(
      tx,
      memberId,
      secondaryId,
      secondary.affiliations.map((a) => a.id),
    )
  }

  // Move memberOrganization roles
  if (secondary.memberOrganizations.length > 0) {
    nonExistingOrgIds = await findNonExistingOrganizationIds(
      tx,
      secondary.memberOrganizations.map((o) => o.organizationId),
    )

    for (const role of secondary.memberOrganizations.filter(
      (r) => !nonExistingOrgIds.includes(r.organizationId),
    )) {
      await addMemberRole(tx, { ...role, memberId: secondaryId })
    }

    // Delete stale roles from primary that aren't in the preview
    const memberOrganizationsMap = await fetchManyMemberOrgsWithOrgData(tx, [memberId])
    const memberOrganizations = memberOrganizationsMap.get(memberId) ?? []

    rolesToDelete = memberOrganizations.filter(
      (r) =>
        r.source !== 'ui' &&
        !primary.memberOrganizations.some(
          (pr) =>
            pr.organizationId === r.organizationId &&
            pr.title === r.title &&
            pr.dateStart === r.dateStart &&
            pr.dateEnd === r.dateEnd,
        ),
    )

    for (const role of rolesToDelete) {
      await removeMemberRole(tx, role)
    }
  }

  // Update primary member scalar fields — only include fields present in the payload
  // to avoid overwriting NOT NULL columns with null when the frontend omits them
  const primaryMember = await updateMember(
    tx,
    memberId,
    Object.fromEntries(
      Object.entries({
        joinedAt: primary.joinedAt,
        attributes: primary.attributes,
        displayName: primary.displayName,
        reach: primary.reach,
        contributions: primary.contributions,
        manuallyChangedFields: primary.manuallyChangedFields,
        manuallyCreated: primary.manuallyCreated,
      }).filter(([, v]) => v !== undefined),
    ) as MemberUpdateInput,
  )

  if (!primaryMember) {
    throw new BadRequestError(`Failed to update member ${memberId}`)
  }

  // Add primary and secondary to no merge so they don't get suggested again
  await addMemberNoMerge(tx, memberId, secondaryId)

  await setMergeAction(tx, MergeActionType.MEMBER, memberId, secondaryId, {
    step: MergeActionStep.UNMERGE_SYNC_DONE,
  })

  return {
    primary: toPreviewMember(
      primaryMember,
      payload.primary.identities,
      payload.primary.affiliations,
      payload.primary.memberOrganizations.filter((r) => !rolesToDelete.some((d) => sameRole(d, r))),
      payload.primary.contributions,
    ),
    secondary: toPreviewMember(
      secondaryRow,
      secondaryIdentities,
      payload.secondary.affiliations,
      payload.secondary.memberOrganizations.filter(
        (r) => !nonExistingOrgIds.includes(r.organizationId),
      ),
      payload.secondary.contributions,
    ),
    movedIdentities: secondaryIdentities,
  }
}

/**
 * Trigger async post-unmerge work via Temporal.
 */
export async function startMemberUnmergeWorkflow(
  temporal: TemporalClient,
  args: {
    primaryId: string
    secondaryId: string
    movedIdentities: IMemberIdentity[]
    primaryDisplayName: string
    secondaryDisplayName: string
    actorId?: string
  },
): Promise<void> {
  await temporal.workflow.start('finishMemberUnmerging', {
    taskQueue: 'entity-merging',
    workflowId: `finishMemberUnmerging/${args.primaryId}/${args.secondaryId}`,
    retry: { maximumAttempts: 10 },
    args: [
      args.primaryId,
      args.secondaryId,
      args.movedIdentities,
      args.primaryDisplayName,
      args.secondaryDisplayName,
      args.actorId,
    ],
    searchAttributes: {
      TenantId: [DEFAULT_TENANT_ID],
    },
  })
}
