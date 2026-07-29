/* eslint-disable no-continue */
import lodash from 'lodash'

import { captureApiChange, memberEditIdentitiesAction } from '@crowd/audit-logs'
import { Error404, Error409 } from '@crowd/common'
import { findIdentitiesForMembers, insertMemberIdentities } from '@crowd/data-access-layer'
import {
  deleteMemberIdentity,
  fetchMemberIdentities,
  findMemberIdentityById,
  findMemberIdentityConflict,
  touchMemberUpdatedAt,
  updateMemberIdentity,
} from '@crowd/data-access-layer/src/members'
import { LoggerBase } from '@crowd/logging'
import { IMemberIdentity, MemberIdentityType, NewMemberIdentity } from '@crowd/types'

import { IRepositoryOptions } from '@/database/repositories/IRepositoryOptions'
import SequelizeRepository from '@/database/repositories/sequelizeRepository'
import { optionsQx } from '@/database/sequelizeQueryExecutor'

import { IServiceOptions } from '../IServiceOptions'

function normalizeIdentityValue(type: string, value: string): string {
  const trimmed = value.trim()
  return type === MemberIdentityType.EMAIL ? trimmed.toLowerCase() : trimmed
}

export default class MemberIdentityService extends LoggerBase {
  options: IServiceOptions

  constructor(options: IServiceOptions) {
    super(options.log)
    this.options = options
  }

  // Member identity list
  async list(memberId: string): Promise<IMemberIdentity[]> {
    const qx = SequelizeRepository.getQueryExecutor(this.options)
    return fetchMemberIdentities(qx, memberId)
  }

  // Member identity creation
  async create(memberId: string, data: NewMemberIdentity): Promise<IMemberIdentity[]> {
    let tx

    try {
      const list = await captureApiChange(
        this.options,
        memberEditIdentitiesAction(memberId, async (captureOldState, captureNewState) => {
          const repoOptions: IRepositoryOptions =
            await SequelizeRepository.createTransactionalRepositoryOptions(this.options)

          const memberIdentities = (
            await findIdentitiesForMembers(optionsQx(repoOptions), [memberId])
          )
            .get(memberId)
            .map((identity) => lodash.omit(identity, ['createdAt', 'integrationId']))

          captureOldState(lodash.sortBy(memberIdentities, [(i) => i.platform, (i) => i.type]))

          tx = repoOptions.transaction

          const qx = SequelizeRepository.getQueryExecutor(repoOptions)

          const identityData = {
            ...data,
            value: normalizeIdentityValue(data.type, data.value),
          }

          // Check if identity already exists
          const conflict = await findMemberIdentityConflict(qx, {
            value: identityData.value,
            platform: identityData.platform,
            type: identityData.type,
          })

          if (conflict) {
            throw new Error409(
              this.options.language,
              'errors.alreadyExists',
              // @ts-ignore
              JSON.stringify({
                memberId: conflict.memberId,
              }),
            )
          }

          // Create member identity
          await insertMemberIdentities(qx, [{ ...identityData, memberId }])

          await touchMemberUpdatedAt(qx, memberId)

          // List all member identities
          const list = await fetchMemberIdentities(qx, memberId)

          captureNewState(lodash.sortBy(list, [(i) => i.platform, (i) => i.type]))

          await SequelizeRepository.commitTransaction(tx)

          return list
        }),
      )

      return list
    } catch (error) {
      if (tx) {
        await SequelizeRepository.rollbackTransaction(tx)
      }

      throw error
    }
  }

  async findById(memberId: string, id: string): Promise<IMemberIdentity> {
    const qx = SequelizeRepository.getQueryExecutor(this.options)
    return findMemberIdentityById(qx, memberId, id)
  }

  // Member multiple identity creation
  async createMultiple(memberId: string, data: NewMemberIdentity[]): Promise<IMemberIdentity[]> {
    let tx

    try {
      const list = await captureApiChange(
        this.options,
        memberEditIdentitiesAction(memberId, async (captureOldState, captureNewState) => {
          const repoOptions: IRepositoryOptions =
            await SequelizeRepository.createTransactionalRepositoryOptions(this.options)

          const memberIdentities = (
            await findIdentitiesForMembers(optionsQx(repoOptions), [memberId])
          )
            .get(memberId)
            .map((identity) => lodash.omit(identity, ['createdAt', 'integrationId']))

          captureOldState(lodash.sortBy(memberIdentities, [(i) => i.platform, (i) => i.type]))

          tx = repoOptions.transaction

          const qx = SequelizeRepository.getQueryExecutor(repoOptions)

          // Check if any of the identities already exist
          const normalizedData = data.map((identity) => ({
            ...identity,
            value: normalizeIdentityValue(identity.type, identity.value),
          }))

          for (const identity of normalizedData) {
            const conflict = await findMemberIdentityConflict(qx, {
              value: identity.value,
              platform: identity.platform,
              type: identity.type,
            })

            if (conflict) {
              throw new Error409(
                this.options.language,
                'errors.alreadyExists',
                // @ts-ignore
                JSON.stringify({
                  memberId: conflict.memberId,
                }),
              )
            }
          }

          // Create member identities
          await insertMemberIdentities(
            qx,
            normalizedData.map((identity) => ({ ...identity, memberId })),
          )

          await touchMemberUpdatedAt(qx, memberId)

          // List all member identities
          const list = await fetchMemberIdentities(qx, memberId)

          captureNewState(lodash.sortBy(list, [(i) => i.platform, (i) => i.type]))

          await SequelizeRepository.commitTransaction(tx)

          return list
        }),
      )

      return list
    } catch (error) {
      if (tx) {
        await SequelizeRepository.rollbackTransaction(tx)
      }

      throw error
    }
  }

  // Update member identity
  async update(
    id: string,
    memberId: string,
    data: Partial<IMemberIdentity>,
  ): Promise<IMemberIdentity[]> {
    let tx

    try {
      const list = await captureApiChange(
        this.options,
        memberEditIdentitiesAction(memberId, async (captureOldState, captureNewState) => {
          const repoOptions: IRepositoryOptions =
            await SequelizeRepository.createTransactionalRepositoryOptions(this.options)

          const memberIdentities = (
            await findIdentitiesForMembers(optionsQx(repoOptions), [memberId])
          )
            .get(memberId)
            .map((identity) => lodash.omit(identity, ['createdAt', 'integrationId']))

          captureOldState(lodash.sortBy(memberIdentities, [(i) => i.platform, (i) => i.type]))

          tx = repoOptions.transaction

          const qx = SequelizeRepository.getQueryExecutor(repoOptions)

          const currentIdentity = memberIdentities.find((identity) => identity.id === id)
          if (!currentIdentity) {
            throw new Error404(this.options.language, 'errors.notFound.message')
          }

          const value = normalizeIdentityValue(
            data.type ?? currentIdentity.type,
            data.value ?? currentIdentity.value,
          )
          const platform = data.platform ?? currentIdentity.platform
          const type = data.type ?? currentIdentity.type

          const conflict = await findMemberIdentityConflict(qx, {
            value,
            platform,
            type,
            excludeMemberId: memberId,
          })

          if (conflict) {
            throw new Error409(
              this.options.language,
              'errors.alreadyExists',
              // @ts-ignore
              JSON.stringify({
                memberId: conflict.memberId,
              }),
            )
          }

          // Update member identity with new data
          await updateMemberIdentity(qx, memberId, id, {
            ...data,
            ...(data.value !== undefined ? { value } : {}),
          })

          await touchMemberUpdatedAt(qx, memberId)

          // List all member identities
          const list = await fetchMemberIdentities(qx, memberId)

          captureNewState(lodash.sortBy(list, [(i) => i.platform, (i) => i.type]))

          await SequelizeRepository.commitTransaction(tx)

          return list
        }),
      )

      return list
    } catch (error) {
      if (tx) {
        await SequelizeRepository.rollbackTransaction(tx)
      }

      throw error
    }
  }

  // Delete member identity
  async delete(id: string, memberId: string): Promise<IMemberIdentity[]> {
    let tx

    try {
      const repoOptions: IRepositoryOptions =
        await SequelizeRepository.createTransactionalRepositoryOptions(this.options)

      tx = repoOptions.transaction

      const qx = SequelizeRepository.getQueryExecutor(repoOptions)

      // Delete member identity
      await deleteMemberIdentity(qx, memberId, id)

      await touchMemberUpdatedAt(qx, memberId)

      // List all member identities
      const list = await fetchMemberIdentities(qx, memberId)

      await SequelizeRepository.commitTransaction(tx)

      return list
    } catch (error) {
      if (tx) {
        await SequelizeRepository.rollbackTransaction(tx)
      }
      throw error
    }
  }
}
