/* eslint-disable no-continue */
import { Transaction } from 'sequelize'

import { Error404 } from '@crowd/common'
import { CommonMemberService } from '@crowd/common_services'
import {
  OrganizationField,
  changeMemberOrganizationAffiliationOverrides,
  checkOrganizationAffiliationPolicy,
  cleanSoftDeletedMemberOrganization,
  createMemberOrganization,
  deleteMemberOrganizations,
  fetchMemberOrganizations,
  findMemberAffiliationOverrides,
  optionsQx,
  queryOrgs,
  updateMemberOrganization,
} from '@crowd/data-access-layer'
import { LoggerBase } from '@crowd/logging'
import {
  IMemberOrganization,
  IOrganization,
  IRenderFriendlyMemberOrganization,
  MemberOrganizationUpdate,
} from '@crowd/types'

import SequelizeRepository from '@/database/repositories/sequelizeRepository'

import { IServiceOptions } from '../IServiceOptions'

type IOrganizationSummary = Pick<IOrganization, 'id' | 'displayName' | 'logo' | 'createdAt'>

export default class MemberOrganizationsService extends LoggerBase {
  options: IServiceOptions

  private readonly commonMemberService: CommonMemberService

  constructor(options: IServiceOptions) {
    super(options.log)
    this.options = options
    this.commonMemberService = new CommonMemberService(
      optionsQx(options),
      options.temporal,
      options.log,
    )
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

    // Create mapping by id to speed up the processing
    const orgByid: Record<string, IOrganizationSummary> = organizations.reduce(
      (obj: Record<string, IOrganizationSummary>, org) => ({
        ...obj,
        [org.id]: org,
      }),
      {},
    )

    // Format the results and order by dateStart and dateEnd
    const allOrganizations = memberOrganizations
      .filter((mo) => orgByid[mo.organizationId]) // Only include non-deleted organizations
      .map((mo) => ({
        ...(orgByid[mo.organizationId] || {}),
        id: mo.organizationId,
        memberOrganizations: {
          ...mo,
          affiliationOverride: affiliationOverrides.find((ao) => ao.memberOrganizationId === mo.id),
        },
      }))
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

      // Clean up any soft-deleted entries
      await cleanSoftDeletedMemberOrganization(qx, memberId, data.organizationId, data)

      // Create new member organization
      const newMemberOrgId = await createMemberOrganization(qx, memberId, data)

      // Check if organization affiliation is blocked
      const isAffiliationBlocked = await checkOrganizationAffiliationPolicy(qx, data.organizationId)

      // If organization affiliation is blocked, create an affiliation override
      if (newMemberOrgId && isAffiliationBlocked) {
        await changeMemberOrganizationAffiliationOverrides(qx, [
          {
            memberId,
            memberOrganizationId: newMemberOrgId,
            allowAffiliation: false,
          },
        ])
      }

      // Start affiliation recalculation within the same transaction
      await this.commonMemberService.startAffiliationRecalculation(memberId, [data.organizationId])

      // Fetch updated list
      const result = await this.list(memberId, transaction)

      await SequelizeRepository.commitTransaction(transaction)
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

      const update: MemberOrganizationUpdate = Object.fromEntries(
        Object.entries({
          organizationId: data.organizationId,
          title: data.title,
          dateStart: data.dateStart,
          dateEnd: data.dateEnd,
          source: data.source,
          verified: data.verified,
          verifiedBy: data.verifiedBy,
        }).filter(([, v]) => v !== undefined),
      )

      await cleanSoftDeletedMemberOrganization(qx, memberId, data.organizationId, data)
      await updateMemberOrganization(qx, memberId, id, update)

      await this.commonMemberService.startAffiliationRecalculation(memberId, [data.organizationId])

      const result = await this.list(memberId, transaction)

      await SequelizeRepository.commitTransaction(transaction)
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

      await deleteMemberOrganizations(qx, memberId, [id], true)

      await this.commonMemberService.startAffiliationRecalculation(
        memberId,
        [memberOrganizationToBeDeleted.organizationId],
        true,
      )

      const result = await this.list(memberId, transaction)

      await SequelizeRepository.commitTransaction(transaction)
      return result
    } catch (error) {
      await SequelizeRepository.rollbackTransaction(transaction)
      throw error
    }
  }
}
