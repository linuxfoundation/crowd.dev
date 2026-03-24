/* eslint-disable no-continue */
import lodash from 'lodash'
import moment from 'moment-timezone'
import validator from 'validator'

import { captureApiChange, memberUnmergeAction } from '@crowd/audit-logs'
import { Error400, calculateReach, getProperDisplayName, isDomainExcluded } from '@crowd/common'
import {
  CommonMemberService,
  getGithubInstallationToken,
  invalidateMemberQueryCache,
  prepareMemberUnmerge,
  startMemberUnmergeWorkflow,
  unmergeMember,
} from '@crowd/common_services'
import {
  fetchMemberBotSuggestionsBySegment,
  fetchMemberIdentities,
  findMemberIdentityById,
  insertMemberSegmentAggregates,
  queryMembersAdvanced,
} from '@crowd/data-access-layer/src/members'
import { QueryExecutor, optionsQx } from '@crowd/data-access-layer/src/queryExecutor'
import { fetchManySegments } from '@crowd/data-access-layer/src/segments'
import { LoggerBase } from '@crowd/logging'
import {
  IMemberIdentity,
  IMemberUnmergeBackup,
  IMemberUnmergePreviewResult,
  IOrganization,
  IUnmergePreviewResult,
  MemberAttributeType,
  MemberIdentityType,
  MergeActionType,
  OrganizationIdentityType,
  SyncMode,
} from '@crowd/types'

import MemberAttributeSettingsRepository from '../database/repositories/memberAttributeSettingsRepository'
import MemberRepository from '../database/repositories/memberRepository'
import { MergeActionsRepository } from '../database/repositories/mergeActionsRepository'
import SequelizeRepository from '../database/repositories/sequelizeRepository'
import {
  BasicMemberIdentity,
  IActiveMemberFilter,
  IMemberMergeSuggestion,
  mapUsernameToIdentities,
} from '../database/repositories/types/memberTypes'
import telemetryTrack from '../segment/telemetryTrack'

import { IServiceOptions } from './IServiceOptions'
import MemberAttributeSettingsService from './memberAttributeSettingsService'
import OrganizationService from './organizationService'
import SearchSyncService from './searchSyncService'
import SettingsService from './settingsService'

export default class MemberService extends LoggerBase {
  options: IServiceOptions

  constructor(options: IServiceOptions) {
    super(options.log)
    this.options = options
  }

  static normalizeIds(ids: unknown): string[] {
    if (typeof ids === 'string') {
      return ids.length > 0 ? [ids] : []
    }

    if (Array.isArray(ids)) {
      return ids.filter((id): id is string => typeof id === 'string' && id.length > 0)
    }

    return []
  }

  /**
   * Validates the attributes against its saved settings.
   *
   * Throws 400 Errors if the attribute does not exist in settings,
   * or if the sent attribute type does not match the type in the settings.
   * Also restructures custom attributes that come only as a value, without platforms.
   *
   * Example custom attributes restructuring
   * {
   *   attributes: {
   *      someAttributeName: 'someValue'
   *   }
   * }
   *
   * This object is transformed into:
   * {
   *   attributes: {
   *     someAttributeName: {
   *        custom: 'someValue'
   *     },
   *   }
   * }
   *
   * @param attributes
   * @returns restructured object
   */
  async validateAttributes(
    attributes: { [key: string]: any },
    transaction = null,
  ): Promise<object> {
    // check attribute exists in memberAttributeSettings

    const memberAttributeSettings = (
      await MemberAttributeSettingsRepository.findAndCountAll(
        {},
        { ...this.options, ...(transaction && { transaction }) },
      )
    ).rows.reduce((acc, attribute) => {
      acc[attribute.name] = attribute
      return acc
    }, {})

    for (const attributeName of Object.keys(attributes)) {
      if (!memberAttributeSettings[attributeName]) {
        this.log.error('Attribute does not exist', {
          attributeName,
          attributes,
        })
        delete attributes[attributeName]
        continue
      }
      if (typeof attributes[attributeName] !== 'object') {
        attributes[attributeName] = {
          custom: attributes[attributeName],
        }
      }

      for (const platform of Object.keys(attributes[attributeName])) {
        if (
          attributes[attributeName][platform] !== undefined &&
          attributes[attributeName][platform] !== null
        ) {
          if (
            !MemberAttributeSettingsService.isCorrectType(
              attributes[attributeName][platform],
              memberAttributeSettings[attributeName].type,
              { options: memberAttributeSettings[attributeName].options },
            )
          ) {
            this.log.error('Failed to validate attributee', {
              attributeName,
              platform,
              attributeValue: attributes[attributeName][platform],
              attributeType: memberAttributeSettings[attributeName].type,
              options: memberAttributeSettings[attributeName].options,
            })
            throw new Error400(
              this.options.language,
              'settings.memberAttributes.wrongType',
              attributeName,
              memberAttributeSettings[attributeName].type,
            )
          }
        }
      }
    }

    return attributes
  }

  /**
   * Sets the attribute.default key as default values of attribute
   * object using the priority array stored in the settings.
   * Throws a 400 Error if priority array does not exist.
   * @param attributes
   * @returns attribute object with default values
   */
  async setAttributesDefaultValues(attributes: object): Promise<object> {
    if (!(await SettingsService.platformPriorityArrayExists(this.options))) {
      throw new Error400(this.options.language, 'settings.memberAttributes.priorityArrayNotFound')
    }

    const priorityArray = this.options.currentTenant.settings[0].get({ plain: true })
      .attributeSettings.priorities

    for (const attributeName of Object.keys(attributes)) {
      const highestPriorityPlatform = MemberService.getHighestPriorityPlatformForAttributes(
        Object.keys(attributes[attributeName]),
        priorityArray,
      )

      if (highestPriorityPlatform !== undefined) {
        attributes[attributeName].default = attributes[attributeName][highestPriorityPlatform]
      } else {
        delete attributes[attributeName]
      }
    }

    return attributes
  }

  /**
   * Returns the highest priority platform from an array of platforms
   * If any of the platforms does not exist in the priority array, returns
   * the first platform sent as the highest priority platform.
   * @param platforms Array of platforms to select the highest priority platform
   * @param priorityArray zero indexed priority array. Lower index means higher priority
   * @returns the highest priority platform or undefined if values are incorrect
   */
  public static getHighestPriorityPlatformForAttributes(
    platforms: string[],
    priorityArray: string[],
  ): string | undefined {
    if (platforms.length <= 0) {
      return undefined
    }
    const filteredPlatforms = priorityArray.filter((i) => platforms.includes(i))
    return filteredPlatforms.length > 0 ? filteredPlatforms[0] : platforms[0]
  }

  /**
   * Upsert a member. If the member exists, it updates it. If it does not exist, it creates it.
   * The update is done with a deep merge of the original and the new member.
   * The member is returned without relations
   * Only the fields that have changed are updated.
   * @param data Data for the member
   * @param existing If the member already exists. If it does not, false. Othwerwise, the member.
   * @returns The created member
   */
  async upsert(
    data,
    existing: boolean | any = false,
    fireCrowdWebhooks: boolean = true,
    syncToOpensearch = true,
  ) {
    const logger = this.options.log
    const searchSyncService = new SearchSyncService(this.options)

    const errorDetails: any = {}

    if (data.identities && data.identities.length > 0) {
      // map identities to username
      const username = {}
      for (const i of data.identities as IMemberIdentity[]) {
        if (!username[i.platform]) {
          username[i.platform] = [] as BasicMemberIdentity[]
        }

        if (!data.platform && i.type === MemberIdentityType.USERNAME) {
          data.platform = i.platform
        }

        username[i.platform].push({
          value: i.value,
          type: i.type,
        })
      }

      data.username = username
    }

    if (!('platform' in data)) {
      throw new Error400(this.options.language, 'activity.platformRequiredWhileUpsert')
    }

    data.username = mapUsernameToIdentities(data.username, data.platform)

    if (!(data.platform in data.username)) {
      throw new Error400(this.options.language, 'activity.platformAndUsernameNotMatching')
    }

    if (!data.displayName) {
      data.displayName = getProperDisplayName(data.username[data.platform][0].username)
    }

    if (!(data.platform in data.username)) {
      throw new Error400(this.options.language, 'activity.platformAndUsernameNotMatching')
    }

    if (!data.displayName) {
      data.displayName = data.username[data.platform].username
    }

    const transaction = await SequelizeRepository.createTransaction(this.options)

    try {
      const { platform } = data

      if (data.attributes) {
        data.attributes = await this.validateAttributes(data.attributes, transaction)
      }

      if (data.reach) {
        data.reach = typeof data.reach === 'object' ? data.reach : { [platform]: data.reach }
        data.reach = calculateReach(data.reach, {})
      } else {
        data.reach = { total: -1 }
      }

      delete data.platform

      if (!('joinedAt' in data)) {
        data.joinedAt = moment.tz('Europe/London').toDate()
      }

      if (!existing) {
        existing = await this.memberExists(data.username, platform)
      } else {
        // let's look just in case for an existing member and if they are different we should log them because they will probably fail to insert
        const tempExisting = await this.memberExists(data.username, platform)

        if (!tempExisting) {
          logger.warn(
            { existingMemberId: existing.id },
            'We have received an existing member but actually we could not find him by username and platform!',
          )
          errorDetails.reason = 'member_service_upsert_existing_member_not_found'
          errorDetails.details = {
            existingMemberId: existing.id,
            username: data.username,
            platform,
          }
        } else if (existing.id !== tempExisting.id) {
          logger.warn(
            { existingMemberId: existing.id, actualExistingMemberId: tempExisting.id },
            'We found a member with the same username and platform but different id!',
          )
          errorDetails.reason = 'member_service_upsert_existing_member_mismatch'
          errorDetails.details = {
            existingMemberId: existing.id,
            actualExistingMemberId: tempExisting.id,
            username: data.username,
            platform,
          }
        }
      }

      // Collect IDs for relation
      const organizations = []
      // If organizations are sent
      if (data.organizations) {
        for (const organization of data.organizations) {
          if (typeof organization === 'string' && validator.isUUID(organization)) {
            // If an ID was already sent, we simply push it to the list
            organizations.push(organization)
          } else if (typeof organization === 'object' && organization.id) {
            organizations.push(organization)
          } else {
            // Otherwise, either another string or an object was sent
            const organizationService = new OrganizationService(this.options)
            let data = {}
            if (typeof organization === 'string') {
              // If a string was sent, we assume it is the name of the organization
              data = {
                identities: [
                  {
                    value: organization,
                    type: OrganizationIdentityType.USERNAME,
                    platform,
                    verified: true,
                    source: 'ui',
                    sourceId: null,
                    integrationId: null,
                  },
                ],
              }
            } else {
              // Otherwise, we assume it is an object with the data of the organization
              data = organization
            }
            // We createOrUpdate the organization and add it to the list of IDs
            const organizationRecord = await organizationService.createOrUpdate(
              data as IOrganization,
              {
                doSync: syncToOpensearch,
                mode: SyncMode.ASYNCHRONOUS,
              },
            )
            organizations.push({ id: organizationRecord.id })
          }
        }
      }

      // Auto assign member to organization if email domain matches
      if (data.emails) {
        const emailDomains = new Set<string>()

        // Collect unique domains
        for (const email of data.emails) {
          if (email) {
            const domain = email.split('@')[1]
            if (!isDomainExcluded(domain)) {
              emailDomains.add(domain)
            }
          }
        }

        // Fetch organization ids for these domains
        const organizationService = new OrganizationService(this.options)
        for (const domain of emailDomains) {
          if (domain) {
            const org = await organizationService.createOrUpdate(
              {
                displayName: domain,
                attributes: {
                  name: {
                    default: domain,
                    custom: [domain],
                  },
                },
                identities: [
                  {
                    value: domain,
                    type: OrganizationIdentityType.PRIMARY_DOMAIN,
                    platform: 'email',
                    verified: true,
                    source: 'ui',
                    sourceId: null,
                    integrationId: null,
                  },
                ],
              },
              {
                doSync: syncToOpensearch,
                mode: SyncMode.ASYNCHRONOUS,
              },
            )

            if (org) {
              organizations.push({ id: org.id })
            }
          }
        }
      }

      // Remove dups
      if (organizations.length > 0) {
        data.organizations = lodash.uniqBy(organizations, 'id')
      }

      let record
      if (existing) {
        const { id } = existing
        delete existing.id
        const toUpdate = CommonMemberService.membersMerge(existing, data)

        if (toUpdate.attributes) {
          toUpdate.attributes = await this.setAttributesDefaultValues(toUpdate.attributes)
        }

        // It is important to call it with doPopulateRelations=false
        // because otherwise the performance is greatly decreased in integrations
        record = await MemberRepository.update(id, toUpdate, {
          ...this.options,
          transaction,
        })
      } else {
        // It is important to call it with doPopulateRelations=false
        // because otherwise the performance is greatly decreased in integrations
        if (data.attributes) {
          data.attributes = await this.setAttributesDefaultValues(data.attributes)
        }

        record = await MemberRepository.create(data, {
          ...this.options,
          transaction,
        })

        telemetryTrack(
          'Member created',
          {
            id: record.id,
            createdAt: record.createdAt,
            identities: record.identities,
          },
          this.options,
        )
      }

      const qx = SequelizeRepository.getQueryExecutor({ ...this.options, transaction })
      await (async function includeMemberInSegments(
        qx: QueryExecutor,
        memberId: string,
        segmentIds: string[],
      ) {
        const segments = await fetchManySegments(qx, segmentIds)
        const data = segments.reduce((acc, s) => {
          for (const segmentId of [s.id, s.parentId, s.grandparentId]) {
            acc.push({
              memberId,
              segmentId,

              activityCount: 0,
              lastActive: '1970-01-01',
              activityTypes: [],
              activeOn: [],
              averageSentiment: null,
            })
          }

          return acc
        }, [])

        await insertMemberSegmentAggregates(qx, data)
      })(
        qx,
        record.id,
        this.options.currentSegments.map((s) => s.id),
      )

      await SequelizeRepository.commitTransaction(transaction)

      if (syncToOpensearch) {
        await searchSyncService.triggerMemberSync(record.id)
      }

      if (!fireCrowdWebhooks) {
        this.log.info('Ignoring outgoing webhooks because of fireCrowdWebhooks!')
      }

      return record
    } catch (error) {
      const reason = errorDetails.reason || undefined
      const details = errorDetails.details || undefined

      if (error.name && error.name.includes('Sequelize')) {
        logger.error(
          error,
          {
            query: error.sql,
            errorMessage: error.original.message,
            reason,
            details,
          },
          'Error during member upsert!',
        )
      } else {
        logger.error(error, { reason, details }, 'Error during member upsert!')
      }

      await SequelizeRepository.rollbackTransaction(transaction)

      SequelizeRepository.handleUniqueFieldError(error, this.options.language, 'member')

      throw { ...error, reason, details }
    }
  }

  /**
   * Checks if given user already exists by username and platform.
   * Username can be given as a plain string or as dictionary with
   * related platforms.
   * Ie:
   * username = 'anil' || username = { github: 'anil' } || username = { github: 'anil', twitter: 'some-other-username' } || username = { github: { username: 'anil' } } || username = { github: [{ username: 'anil' }] }
   * @param username username of the member
   * @param platform platform of the member
   * @returns null | found member
   */
  async memberExists(username: object | string, platform: string) {
    const fillRelations = false

    const usernames: string[] = []

    if (typeof username === 'string') {
      usernames.push(username)
    } else if (typeof username === 'object') {
      if ('username' in username) {
        usernames.push((username as any).username)
      } else if (platform in username) {
        if (typeof username[platform] === 'string') {
          usernames.push(username[platform])
        } else if (Array.isArray(username[platform])) {
          if (username[platform].length === 0) {
            throw new Error400(this.options.language, 'activity.platformAndUsernameNotMatching')
          } else if (typeof username[platform] === 'string') {
            usernames.push(username[platform])
          } else if (typeof username[platform][0] === 'object') {
            usernames.push(...username[platform].map((u) => u.username))
          }
        } else if (typeof username[platform] === 'object') {
          usernames.push(username[platform].username)
        } else {
          throw new Error400(this.options.language, 'activity.platformAndUsernameNotMatching')
        }
      } else {
        throw new Error400(this.options.language, 'activity.platformAndUsernameNotMatching')
      }
    }

    // It is important to call it with doPopulateRelations=false
    // because otherwise the performance is greatly decreased in integrations
    const existing = await MemberRepository.memberExists(
      usernames,
      platform,
      {
        ...this.options,
      },
      fillRelations,
    )

    return existing
  }

  async unmergePreview(
    memberId: string,
    identityId: string,
    revertPreviousMerge = false,
  ): Promise<IUnmergePreviewResult<IMemberUnmergePreviewResult>> {
    const qx = SequelizeRepository.getQueryExecutor(this.options)
    return prepareMemberUnmerge(qx, memberId, identityId, revertPreviousMerge)
  }

  async unmerge(
    memberId: string,
    payload: IUnmergePreviewResult<IMemberUnmergePreviewResult>,
  ): Promise<void> {
    const qx = SequelizeRepository.getQueryExecutor(this.options)

    const { primary, secondary, movedIdentities } = await captureApiChange(
      this.options,
      memberUnmergeAction(memberId, async (captureOldState, captureNewState) => {
        captureOldState({ primary: payload.primary })

        const result = await qx.tx(async (tx) =>
          unmergeMember(tx, memberId, payload, this.options.currentUser?.id),
        )

        captureNewState({
          primary: result.primary,
          secondary: result.secondary,
        })

        return result
      }),
    )

    await invalidateMemberQueryCache(this.options.redis, [primary.id, secondary.id], true)

    await startMemberUnmergeWorkflow(this.options.temporal, {
      primaryId: primary.id,
      secondaryId: secondary.id,
      movedIdentities,
      primaryDisplayName: primary.displayName,
      secondaryDisplayName: secondary.displayName,
      actorId: this.options.currentUser?.id,
    })
  }

  async canRevertMerge(memberId: string, identityId: string): Promise<boolean> {
    try {
      const qx = SequelizeRepository.getQueryExecutor(this.options)

      const identity = await findMemberIdentityById(qx, memberId, identityId)

      if (!identity) {
        throw new Error(`Member doesn't have an identity with id ${identityId}!`)
      }

      const mergeAction = await MergeActionsRepository.findMergeBackup(
        memberId,
        MergeActionType.MEMBER,
        identity,
        this.options,
      )

      if (!mergeAction) {
        return false
      }

      const secondaryBackup = mergeAction.unmergeBackup.secondary as IMemberUnmergeBackup

      const memberIdentities = await fetchMemberIdentities(qx, memberId)

      const remainingIdentitiesInCurrentMember = memberIdentities.filter(
        (i) =>
          !secondaryBackup.identities.some(
            (s) => s.platform === i.platform && s.value === i.value && s.type === i.type,
          ),
      )

      return remainingIdentitiesInCurrentMember.length > 0
    } catch (err) {
      this.options.log.error(err, 'Error while checking if member merge can be reverted!')
      throw err
    }
  }

  static MEMBER_MERGE_FIELDS = [
    'id',
    'reach',
    'tasks',
    'joinedAt',
    'tenantId',
    'attributes',
    'displayName',
    'affiliations',
    'contributions',
    'manuallyCreated',
    'manuallyChangedFields',
  ]

  async findGithub(memberId: string) {
    const qx = SequelizeRepository.getQueryExecutor(this.options)
    const memberIdentities = MemberRepository.getUsernameFromIdentities(
      await fetchMemberIdentities(qx, memberId),
    )

    const token = await getGithubInstallationToken()
    const axios = require('axios')
    // GitHub allows a maximum of 5 parameters
    const identities = Object.values(memberIdentities).flat().slice(0, 5)
    // Join the usernames for search
    const identitiesQuery = identities.join('+OR+')
    const url = `https://api.github.com/search/users?q=${identitiesQuery}`
    const headers = {
      Accept: 'application/vnd.github+json',
      Authorization: `Bearer ${token}`,
      'X-GitHub-Api-Version': '2022-11-28',
    }
    const response = await axios.get(url, { headers })
    const data = response.data.items.map((item) => ({
      username: item.login,
      avatarUrl: item.avatar_url,
      score: item.score,
      url: item.html_url,
    }))
    return data
  }

  /**
   * Given two members, add them to the toMerge fields of each other.
   * It will also update the tenant's toMerge list, removing any entry that contains
   * the pair.
   * @returns Success/Error message
   */
  async addToMerge(suggestions: IMemberMergeSuggestion[]) {
    const transaction = await SequelizeRepository.createTransaction(this.options)
    try {
      const searchSyncService = new SearchSyncService(this.options)

      await MemberRepository.addToMerge(suggestions, { ...this.options, transaction })
      await SequelizeRepository.commitTransaction(transaction)

      for (const suggestion of suggestions) {
        await searchSyncService.triggerMemberSync(suggestion.members[0])
        await searchSyncService.triggerMemberSync(suggestion.members[1])
      }
      return { status: 200 }
    } catch (error) {
      await SequelizeRepository.rollbackTransaction(transaction)
      this.log.error(error, 'Error while adding members to merge')
      throw error
    }
  }

  /**
   * Given two members, add them to the noMerge fields of each other.
   * @param memberOneId ID of the first member
   * @param memberTwoId ID of the second member
   * @returns Success/Error message
   */
  async addToNoMerge(memberOneId, memberTwoId) {
    const transaction = await SequelizeRepository.createTransaction(this.options)

    try {
      await MemberRepository.addNoMerge(memberOneId, memberTwoId, {
        ...this.options,
        transaction,
      })
      await MemberRepository.addNoMerge(memberTwoId, memberOneId, {
        ...this.options,
        transaction,
      })
      await MemberRepository.removeToMerge(memberOneId, memberTwoId, {
        ...this.options,
        transaction,
      })
      await MemberRepository.removeToMerge(memberTwoId, memberOneId, {
        ...this.options,
        transaction,
      })

      await SequelizeRepository.commitTransaction(transaction)

      return { status: 200 }
    } catch (error) {
      await SequelizeRepository.rollbackTransaction(transaction)

      throw error
    }
  }

  async update(
    id,
    data,
    {
      syncToOpensearch = true,
      manualChange = false,
      invalidateCache = false,
    }: {
      syncToOpensearch?: boolean
      manualChange?: boolean
      invalidateCache?: boolean
    } = {},
  ) {
    let transaction
    try {
      const repoOptions = await SequelizeRepository.createTransactionalRepositoryOptions(
        this.options,
      )
      transaction = repoOptions.transaction

      if (data.displayName) {
        data.displayName = getProperDisplayName(data.displayName)
      }

      const record = await MemberRepository.update(id, data, repoOptions, {
        manualChange,
      })

      await SequelizeRepository.commitTransaction(transaction)

      // Invalidate member query cache after update
      // Pass invalidateCache from options to control whether to clear list caches
      await invalidateMemberQueryCache(this.options.redis, [id], invalidateCache)

      const commonMemberService = new CommonMemberService(
        optionsQx(this.options),
        this.options.temporal,
        this.options.log,
      )
      await commonMemberService.startAffiliationRecalculation(
        id,
        (data.organizations || []).map((o) => o.id),
        syncToOpensearch,
      )

      return record
    } catch (error) {
      if (error.name && error.name.includes('Sequelize')) {
        this.log.error(
          error,
          {
            query: error.sql,
            errorMessage: error.original.message,
          },
          'Error during member update!',
        )
      } else {
        this.log.error(error, 'Error during member update!')
      }

      if (transaction) {
        await SequelizeRepository.rollbackTransaction(transaction)
      }

      SequelizeRepository.handleUniqueFieldError(error, this.options.language, 'member')

      throw error
    }
  }

  async destroyBulk(ids: unknown) {
    const normalizedIds: string[] = MemberService.normalizeIds(ids)

    if (normalizedIds.length === 0) {
      return
    }

    const transaction = await SequelizeRepository.createTransaction(this.options)
    const searchSyncService = new SearchSyncService(this.options)

    try {
      await MemberRepository.destroyBulk(
        normalizedIds,
        {
          ...this.options,
          transaction,
        },
        true,
      )

      await SequelizeRepository.commitTransaction(transaction)

      // Invalidate member query cache after bulk delete
      // Pass invalidateAll=true to also clear list caches since deletion affects list views
      await invalidateMemberQueryCache(this.options.redis, normalizedIds, true)
    } catch (error) {
      await SequelizeRepository.rollbackTransaction(transaction)
      throw error
    }

    for (const id of normalizedIds) {
      await searchSyncService.triggerRemoveMember(id)
    }
  }

  async destroyAll(ids: unknown) {
    const normalizedIds: string[] = MemberService.normalizeIds(ids)

    if (normalizedIds.length === 0) {
      return
    }

    const transaction = await SequelizeRepository.createTransaction(this.options)
    const searchSyncService = new SearchSyncService(this.options)

    try {
      for (const id of normalizedIds) {
        await MemberRepository.destroy(
          id,
          {
            ...this.options,
            transaction,
          },
          true,
        )
      }

      await SequelizeRepository.commitTransaction(transaction)

      // Invalidate member query cache after deletion
      // Pass invalidateAll=true to also clear list caches since deletion affects list views
      await invalidateMemberQueryCache(this.options.redis, normalizedIds, true)
    } catch (error) {
      await SequelizeRepository.rollbackTransaction(transaction)
      throw error
    }

    for (const id of normalizedIds) {
      await searchSyncService.triggerRemoveMember(id)
    }
  }

  async findById(
    id,
    segmentId?: string,
    include: Record<string, boolean> = {},
    includeAllAttributes = false,
  ) {
    return MemberRepository.findById(
      id,
      this.options,
      {
        segmentId,
      },
      include,
      includeAllAttributes,
    )
  }

  async findAllAutocomplete(data) {
    const qx = optionsQx(this.options)
    const bgQx = optionsQx({ ...this.options, transaction: null })

    return queryMembersAdvanced(qx, bgQx, this.options.redis, {
      filter: data.filter,
      offset: data.offset,
      orderBy: data.orderBy,
      limit: data.limit,
      segmentId: data.segments[0],
      include: {
        segments: true,
      },
    })
  }

  async findAndCountActive(
    filters: IActiveMemberFilter,
    offset: number,
    limit: number,
    orderBy: string,
    segments: string[],
  ) {
    const memberAttributeSettings = (
      await MemberAttributeSettingsRepository.findAndCountAll({}, this.options)
    ).rows

    return MemberRepository.findAndCountActiveOpensearch(
      filters,
      limit,
      offset,
      orderBy,
      this.options,
      memberAttributeSettings,
      segments,
    )
  }

  async query(data, exportMode = false) {
    const memberAttributeSettings = (
      await MemberAttributeSettingsRepository.findAndCountAll({}, this.options)
    ).rows.filter((setting) => setting.type !== MemberAttributeType.SPECIAL)

    const segmentId = (data.segments || [])[0]

    if (!segmentId) {
      throw new Error400(this.options.language, 'member.segmentsRequired')
    }

    const qx = optionsQx(this.options)
    const bgQx = optionsQx({ ...this.options, transaction: null })

    return queryMembersAdvanced(qx, bgQx, this.options.redis, {
      ...data,
      segmentId,
      attributesSettings: memberAttributeSettings,
      include: {
        memberOrganizations: true,
        lfxMemberships: true,
        identities: true,
        attributes: true,
        maintainers: true,
      },
      exportMode,
    })
  }

  async queryForCsv(data) {
    data.limit = 10000000000000
    const found = await this.query(data, true)

    const relations = [{ relation: 'organizations', attributes: ['name'] }]
    for (const relation of relations) {
      for (const member of found.rows) {
        member[relation.relation] = member[relation.relation]?.map((i) => ({
          id: i.id,
          ...lodash.pick(i, relation.attributes),
        }))
      }
    }

    return found
  }

  async findMembersWithMergeSuggestions(args) {
    return MemberRepository.findMembersWithMergeSuggestions(args, this.options)
  }

  async findMembersWithBotSuggestions(args) {
    const segments = SequelizeRepository.getSegmentIds(this.options)

    const segmentId = segments?.length > 0 ? segments[0] : null

    if (!segmentId) {
      throw new Error400(this.options.language, 'member.segmentsRequired')
    }

    const qx = SequelizeRepository.getQueryExecutor(this.options)
    return fetchMemberBotSuggestionsBySegment(qx, segmentId, args.limit ?? 10, args.offset ?? 0)
  }
}
