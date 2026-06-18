/* eslint-disable no-continue */
import lodash from 'lodash'
import { Transaction } from 'sequelize'

import { Error404, sanitizeMemberOrganizationDateRange } from '@crowd/common'
import { signalMemberUpdate } from '@crowd/common_services'
import {
  OrganizationField,
  changeMemberOrganizationAffiliationOverrides,
  cleanSoftDeletedMemberOrganization,
  createMemberOrganization,
  deleteMemberOrganizations,
  fetchManyOrganizationAffiliationPolicies,
  fetchMemberOrganizationById,
  fetchMemberOrganizations,
  findMemberAffiliationOverrides,
  queryOrgs,
  updateMemberOrganization,
} from '@crowd/data-access-layer'
import { deleteMemberSegmentAffiliations } from '@crowd/data-access-layer/src/member_segment_affiliations'
import { LoggerBase } from '@crowd/logging'
import {
  IMemberOrganization,
  IOrganization,
  IRenderFriendlyMemberOrganization,
  MemberOrganizationUpdate,
  OrganizationSource,
} from '@crowd/types'

import SequelizeRepository from '@/database/repositories/sequelizeRepository'
import {
  getOverlappingEmailDomainMemberOrganizations,
  groupMemberOrganizations,
} from '@/utils/mapper'

import { IServiceOptions } from '../IServiceOptions'

type IOrganizationSummary = Pick<IOrganization, 'id' | 'displayName' | 'logo' | 'createdAt'>

export default class MemberOrganizationsService extends LoggerBase {
  options: IServiceOptions

  constructor(options: IServiceOptions) {
    super(options.log)
    this.options = options
  }

  // Member organization list
  async list(
    memberId: string,
    transaction?: Transaction,
  ): Promise<IRenderFriendlyMemberOrganization[]> {
    const qx = SequelizeRepository.getQueryExecutor({ ...this.options, transaction })

    // Fetch member organizations
    const memberOrganizations: IMemberOrganization[] = await fetchMemberOrganizations(qx, memberId)

    if (memberOrganizations.length === 0) {
      return []
    }

    // Parse unique organization ids
    const orgIds: string[] = [...new Set(memberOrganizations.map((mo) => mo.organizationId))]

    // Fetch organizations
    let organizations: IOrganizationSummary[] = []
    if (orgIds.length) {
      organizations = await queryOrgs(qx, {
        filter: {
          [OrganizationField.ID]: {
            in: orgIds,
          },
        },
        fields: [
          OrganizationField.ID,
          OrganizationField.DISPLAY_NAME,
          OrganizationField.LOGO,
          OrganizationField.CREATED_AT,
        ],
      })
    }

    // Fetch affiliation overrides
    const affiliationOverrides = await findMemberAffiliationOverrides(
      qx,
      memberId,
      memberOrganizations.map((mo) => mo.id),
    )

    const overridesByMemberOrganizationId = new Map(
      affiliationOverrides.map((override) => [override.memberOrganizationId, override]),
    )

    // Create mapping by id to speed up the processing
    const orgById: Record<string, IOrganizationSummary> = organizations.reduce(
      (obj: Record<string, IOrganizationSummary>, org) => ({
        ...obj,
        [org.id]: org,
      }),
      {},
    )

    // Format the results and order by dateStart and dateEnd
    const groupedMemberOrganizations = groupMemberOrganizations(memberOrganizations)

    const allOrganizations = groupedMemberOrganizations
      .filter((mo): mo is typeof mo & { id: string } => !!mo.id && !!orgById[mo.organizationId])
      .map((mo) => {
        const overlappingEmailDomainRows = getOverlappingEmailDomainMemberOrganizations(
          memberOrganizations,
          mo,
        )

        const relatedIds = [mo.id, ...overlappingEmailDomainRows.map((row) => row.id)]

        const relatedOverrides = relatedIds.map((memberOrganizationId) =>
          overridesByMemberOrganizationId.get(memberOrganizationId),
        )

        const resolvedOverrides = relatedOverrides.filter((override) => !!override)

        // Merge override flags from rows that are displayed as one work experience
        const allowAffiliation =
          resolvedOverrides.length === 0 ||
          resolvedOverrides.every((override) => override.allowAffiliation !== false)

        const isPrimaryWorkExperience = resolvedOverrides.some(
          (override) => override.isPrimaryWorkExperience,
        )

        return {
          ...orgById[mo.organizationId],
          id: mo.organizationId,
          memberOrganizations: {
            ...mo,
            affiliationOverride: {
              memberId,
              memberOrganizationId: mo.id,
              allowAffiliation,
              isPrimaryWorkExperience,
            },
          },
        }
      })
      .sort((a, b) => {
        if (!a || !b) {
          return 0
        }

        // Sort by dateStart (newest first), then by dateEnd (active first - null dateEnd comes first)
        const aDateStart = a.memberOrganizations.dateStart
          ? new Date(a.memberOrganizations.dateStart).getTime()
          : 0
        const bDateStart = b.memberOrganizations.dateStart
          ? new Date(b.memberOrganizations.dateStart).getTime()
          : 0

        if (aDateStart !== bDateStart) {
          return bDateStart - aDateStart // Newest dateStart first
        }

        // If dateStart is the same, prioritize active memberships (null dateEnd)
        const aDateEnd = a.memberOrganizations.dateEnd
        const bDateEnd = b.memberOrganizations.dateEnd

        if (!aDateEnd && bDateEnd) return -1 // a is active, b is not
        if (aDateEnd && !bDateEnd) return 1 // b is active, a is not

        // Both have null dateEnd and dateStart - sort by createdAt, then alphabetically
        if (!aDateEnd && !bDateEnd && aDateStart === 0 && bDateStart === 0) {
          // First try to sort by createdAt
          const aCreatedAt = a.createdAt ? new Date(a.createdAt).getTime() : 0
          const bCreatedAt = b.createdAt ? new Date(b.createdAt).getTime() : 0

          if (aCreatedAt !== bCreatedAt) {
            return bCreatedAt - aCreatedAt // Newest createdAt first
          }

          // If createdAt is also the same, sort alphabetically by displayName
          const aName = (a.displayName || '').toLowerCase()
          const bName = (b.displayName || '').toLowerCase()
          return aName.localeCompare(bName)
        }

        if (!aDateEnd && !bDateEnd) return 0 // both are active with same dateStart

        // Both have dateEnd, sort by dateEnd (newest first)
        return new Date(bDateEnd).getTime() - new Date(aDateEnd).getTime()
      })

    return allOrganizations
  }

  // Member organization creation
  async create(
    memberId: string,
    data: Partial<IMemberOrganization>,
  ): Promise<IRenderFriendlyMemberOrganization[]> {
    const transaction = await SequelizeRepository.createTransaction(this.options)
    const repositoryOptions = { ...this.options, transaction }

    try {
      const qx = SequelizeRepository.getQueryExecutor(repositoryOptions)
      const dates = sanitizeMemberOrganizationDateRange(data.dateStart, data.dateEnd, true)
      const memberOrgData: Partial<IMemberOrganization> = {
        ...data,
        dateStart: dates.dateStart,
        dateEnd: dates.dateEnd,
      }

      // Clean up any soft-deleted entries
      await cleanSoftDeletedMemberOrganization(qx, memberId, data.organizationId, memberOrgData)

      // Create new member organization
      const newMemberOrgId = await createMemberOrganization(qx, memberId, memberOrgData)

      const orgAffiliationPolicyById = await fetchManyOrganizationAffiliationPolicies(qx, [
        data.organizationId,
      ])

      if (newMemberOrgId && orgAffiliationPolicyById.get(data.organizationId)) {
        await changeMemberOrganizationAffiliationOverrides(qx, [
          {
            memberId,
            memberOrganizationId: newMemberOrgId,
            allowAffiliation: false,
          },
        ])
        await deleteMemberSegmentAffiliations(qx, { memberId, organizationId: data.organizationId })
      }

      // Fetch updated list
      const result = await this.list(memberId, transaction)

      await SequelizeRepository.commitTransaction(transaction)

      // Signal after commit so the workflow sees persisted changes
      await signalMemberUpdate(this.options.temporal, memberId, {
        memberOrganizationIds: [data.organizationId],
      })

      return result
    } catch (error) {
      await SequelizeRepository.rollbackTransaction(transaction)
      throw error
    }
  }

  // Update member organization
  async update(
    id: string,
    memberId: string,
    data: Partial<IMemberOrganization>,
  ): Promise<IRenderFriendlyMemberOrganization[]> {
    const transaction = await SequelizeRepository.createTransaction(this.options)
    const repositoryOptions = { ...this.options, transaction }

    try {
      const qx = SequelizeRepository.getQueryExecutor(repositoryOptions)

      const existing = await fetchMemberOrganizationById(qx, id)
      if (!existing || existing.memberId !== memberId) {
        throw new Error404(`Member organization with id ${id} not found!`)
      }

      const hasDateStart = data.dateStart !== undefined
      const hasDateEnd = data.dateEnd !== undefined
      const targetDateRange = sanitizeMemberOrganizationDateRange(
        hasDateStart ? data.dateStart : existing.dateStart,
        hasDateEnd ? data.dateEnd : existing.dateEnd,
        true,
      )

      const update = lodash.pickBy(
        {
          organizationId: data.organizationId,
          title: data.title,
          dateStart: hasDateStart ? targetDateRange.dateStart : undefined,
          dateEnd: hasDateEnd ? targetDateRange.dateEnd : undefined,

          verified: data.verified,
          verifiedBy: data.verifiedBy,
        },
        (v) => v !== undefined,
      ) as MemberOrganizationUpdate

      await cleanSoftDeletedMemberOrganization(qx, memberId, data.organizationId, update)
      await updateMemberOrganization(qx, memberId, id, {
        ...update,
        source: OrganizationSource.UI,
      })

      const memberOrganizations = await fetchMemberOrganizations(qx, memberId)

      const overlapBasis = { ...existing, ...update }

      const overlappingEmailDomainRows = getOverlappingEmailDomainMemberOrganizations(
        memberOrganizations,
        overlapBasis,
      )

      const groupedUpdate = lodash.pickBy(
        {
          // Keep grouped rows aligned for shared display fields; dates stay on the edited row
          title: data.title,
          verified: data.verified,
          verifiedBy: data.verifiedBy,
        },
        (value) => value !== undefined,
      ) as MemberOrganizationUpdate

      if (overlappingEmailDomainRows.length > 0 && Object.keys(groupedUpdate).length > 0) {
        for (const overlappingRow of overlappingEmailDomainRows) {
          if (!overlappingRow.id) {
            continue
          }

          await updateMemberOrganization(qx, memberId, overlappingRow.id, groupedUpdate)
        }
      }

      // Trigger recalculation for old and new orgs if changed
      const orgsToRecalculate = Array.from(
        new Set([existing.organizationId, data.organizationId]),
      ).filter((orgId): orgId is string => Boolean(orgId))

      const result = await this.list(memberId, transaction)

      await SequelizeRepository.commitTransaction(transaction)

      // Signal after commit so the workflow sees persisted changes
      await signalMemberUpdate(this.options.temporal, memberId, {
        memberOrganizationIds: orgsToRecalculate,
      })

      return result
    } catch (error) {
      await SequelizeRepository.rollbackTransaction(transaction)
      throw error
    }
  }

  // Delete member organization
  async delete(id: string, memberId: string): Promise<IRenderFriendlyMemberOrganization[]> {
    const transaction = await SequelizeRepository.createTransaction(this.options)
    const repositoryOptions = { ...this.options, transaction }

    try {
      const qx = SequelizeRepository.getQueryExecutor(repositoryOptions)

      const existingMemberOrganizations = await fetchMemberOrganizations(qx, memberId)
      const memberOrganizationToBeDeleted = existingMemberOrganizations.find((mo) => mo.id === id)

      if (!memberOrganizationToBeDeleted) {
        throw new Error404(`Member organization with id ${id} not found!`)
      }

      const overlappingEmailDomainRows = getOverlappingEmailDomainMemberOrganizations(
        existingMemberOrganizations,
        memberOrganizationToBeDeleted,
      )

      const memberOrganizationIdsToDelete = [
        id,
        ...overlappingEmailDomainRows.flatMap((row) => (row.id ? [row.id] : [])),
      ]

      // Delete hidden grouped rows with the visible row so list responses stay consistent
      await deleteMemberOrganizations(qx, memberId, memberOrganizationIdsToDelete, true)

      const result = await this.list(memberId, transaction)

      await SequelizeRepository.commitTransaction(transaction)

      // Signal after commit so the workflow sees persisted changes
      await signalMemberUpdate(this.options.temporal, memberId, {
        memberOrganizationIds: [memberOrganizationToBeDeleted.organizationId],
        syncToOpensearch: true,
      })

      return result
    } catch (error) {
      await SequelizeRepository.rollbackTransaction(transaction)
      throw error
    }
  }
}
