import isEqual from 'lodash.isequal'
import mergeWith from 'lodash.mergewith'
import uniqby from 'lodash.uniqby'

import {
  ApplicationError,
  DEFAULT_TENANT_ID,
  getEarliestValidDate,
  getProperDisplayName,
  isDomainExcluded,
  isEmail,
  isObjectEmpty,
  singleOrDefault,
} from '@crowd/common'
import { BotDetectionService } from '@crowd/common_services'
import { QueryExecutor, createMember, dbStoreQx, updateMember } from '@crowd/data-access-layer'
import { findIdentitiesForMembers, findMembersByVerifiedUsernames } from '@crowd/data-access-layer'
import { DbStore } from '@crowd/data-access-layer/src/database'
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

import { IMemberCreateData, IMemberUpdateData } from './member.data'
import MemberAttributeService from './memberAttribute.service'
import { OrganizationService } from './organization.service'

export default class MemberService extends LoggerBase {
  private readonly memberRepo: MemberRepository
  private readonly pgQx: QueryExecutor
  private readonly botDetectionService: BotDetectionService

  constructor(
    private readonly store: DbStore,
    private readonly redisClient: RedisClient,
    private readonly temporal: TemporalClient,
    parentLog: Logger,
  ) {
    super(parentLog)

    this.memberRepo = new MemberRepository(store, this.log)
    this.botDetectionService = new BotDetectionService(this.log)
    this.pgQx = dbStoreQx(this.store)
  }

  public async create(
    segmentIds: string[],
    integrationId: string,
    data: IMemberCreateData,
    source: string,
    releaseMemberLock?: () => Promise<void>,
  ): Promise<string> {
    return logExecutionTimeV2(
      async () => {
        try {
          this.log.debug('Creating a new member!')

          // prevent empty identity handles
          data.identities = data.identities.filter((i) => i.value)

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
              () =>
                memberAttributeService.validateAttributes(data.attributes, source as PlatformType),
              this.log,
              'memberService -> create -> validateAttributes',
            )

            attributes = await logExecutionTimeV2(
              () => memberAttributeService.setAttributesDefaultValues(attributes),
              this.log,
              'memberService -> create -> setAttributesDefaultValues',
            )
          }

          // validate emails
          data.identities = this.validateEmails(data.identities)

          data.displayName = getProperDisplayName(data.displayName)

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

          try {
            await logExecutionTimeV2(
              () => this.memberRepo.insertIdentities(id, integrationId, data.identities),
              this.log,
              'memberService -> create -> insertIdentities',
            )
          } catch (err) {
            this.log.error(err, { memberId: id }, 'Error while inserting identities!')
            await logExecutionTimeV2(
              async () => this.memberRepo.destroyMemberAfterError(id, false),
              this.log,
              'memberService -> create -> destroyMemberAfterError',
            )
            throw new ApplicationError('Error while inserting identities for a new member!', err)
          }

          try {
            await logExecutionTimeV2(
              () => this.memberRepo.addToSegments(id, segmentIds),
              this.log,
              'memberService -> create -> addToSegments',
            )
          } catch (err) {
            this.log.error(err, { memberId: id }, 'Error while adding member to segments!')
            await logExecutionTimeV2(
              async () => this.memberRepo.destroyMemberAfterError(id, true),
              this.log,
              'memberService -> create -> destroyMemberAfterError',
            )
            throw err
          }

          if (releaseMemberLock) {
            await releaseMemberLock()
          }

          // we should prevent organization creation for bot members
          if (botDetection === MemberBotDetection.CONFIRMED_BOT) {
            this.log.debug('Skipping organization creation for bot member')
            return id
          }

          // trigger LLM validation if the member is suspected as a bot
          if (botDetection === MemberBotDetection.SUSPECTED_BOT) {
            this.log.debug({ memberId: id }, 'Member suspected as bot. Triggering LLM validation.')
            await this.startMemberBotAnalysisWithLLMWorkflow(id)
          }

          const organizations = []
          const orgService = new OrganizationService(this.store, this.log)
          if (data.organizations) {
            for (const org of data.organizations) {
              const id = await logExecutionTimeV2(
                () => orgService.findOrCreate(source, integrationId, org),
                this.log,
                'memberService -> create -> findOrCreateOrg',
              )
              organizations.push({
                id,
                source: org.source,
              })
            }
          }

          const emailIdentities = data.identities.filter(
            (i) => i.type === MemberIdentityType.EMAIL && i.verified,
          )
          if (emailIdentities.length > 0) {
            const orgs = await logExecutionTimeV2(
              () =>
                this.assignOrganizationByEmailDomain(
                  integrationId,
                  emailIdentities.map((i) => i.value),
                ),
              this.log,
              'memberService -> create -> assignOrganizationByEmailDomain',
            )
            if (orgs.length > 0) {
              organizations.push(...orgs)
            }
          }

          if (organizations.length > 0) {
            const uniqOrgs = uniqby(organizations, 'id')
            const orgService = new OrganizationService(this.store, this.log)

            const orgsToAdd = (
              await Promise.all(
                uniqOrgs.map(async (org) => {
                  // Check if the org was already added to the member in the past, including deleted ones.
                  // If it was, we ignore this org to prevent from adding it again.
                  const existingMemberOrgs = await logExecutionTimeV2(
                    () => orgService.findMemberOrganizations(id, org.id),
                    this.log,
                    'memberService -> create -> findMemberOrganizations',
                  )
                  return existingMemberOrgs.length > 0 ? null : org
                }),
              )
            ).filter((org) => org !== null)

            if (orgsToAdd.length > 0) {
              await logExecutionTimeV2(
                () => orgService.addToMember(segmentIds, id, orgsToAdd),
                this.log,
                'memberService -> create -> addToMember',
              )
            }
          }

          return id
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
    source: string,
    releaseMemberLock?: () => Promise<void>,
  ): Promise<void> {
    await logExecutionTimeV2(
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
              () =>
                memberAttributeService.validateAttributes(data.attributes, source as PlatformType),
              this.log,
              'memberService -> update -> validateAttributes',
            )
          }

          // prevent empty identity handles
          data.identities = data.identities.filter((i) => i.value)

          // validate emails
          data.identities = this.validateEmails(data.identities)

          // make sure displayName is proper
          if (data.displayName) {
            data.displayName = getProperDisplayName(data.displayName)
          }

          const toUpdate = this.mergeData(original, originalIdentities, data)

          if (toUpdate.attributes) {
            this.log.trace({ memberId: id }, 'Setting attribute default values!')
            toUpdate.attributes = await logExecutionTimeV2(
              () => memberAttributeService.setAttributesDefaultValues(toUpdate.attributes),
              this.log,
              'memberService -> update -> setAttributesDefaultValues',
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
                'memberService -> update -> update',
              )
            }

            this.log.trace({ memberId: id }, 'Updating member segment association data in db!')
            await logExecutionTimeV2(
              () => this.memberRepo.addToSegments(id, [segmentId]),
              this.log,
              'memberService -> update -> addToSegment',
            )
          } else {
            this.log.debug({ memberId: id }, 'Nothing to update in a member!')
            await logExecutionTimeV2(
              () => this.memberRepo.addToSegments(id, [segmentId]),
              this.log,
              'memberService -> update -> addToSegment',
            )
          }

          if (identitiesToCreate) {
            this.log.trace({ memberId: id }, 'Inserting new identities!')
            try {
              await logExecutionTimeV2(
                () => this.memberRepo.insertIdentities(id, integrationId, identitiesToCreate),
                this.log,
                'memberService -> update -> insertIdentities',
              )
            } catch (err) {
              throw new ApplicationError(
                'Error while inserting member identities for an existing member!',
                err,
              )
            }
          }

          if (identitiesToUpdate) {
            this.log.trace({ memberId: id }, 'Updating identities!')
            try {
              await logExecutionTimeV2(
                () => this.memberRepo.updateIdentities(id, identitiesToUpdate),
                this.log,
                'memberService -> update -> updateIdentities',
              )
            } catch (err) {
              throw new ApplicationError(
                'Error while updating member identities for an existing member!',
                err,
              )
            }
          }

          if (releaseMemberLock) {
            await releaseMemberLock()
          }

          if (this.botDetectionService.isFlaggedAsBot(toUpdate.attributes)) {
            this.log.debug({ memberId: id }, 'Skipping organization creation for bot member')
            return
          }

          const organizations = []
          const orgService = new OrganizationService(this.store, this.log)
          if (data.organizations) {
            for (const org of data.organizations) {
              this.log.trace({ memberId: id }, 'Finding or creating organization!')

              const orgId = await logExecutionTimeV2(
                () => orgService.findOrCreate(source, integrationId, org),
                this.log,
                'memberService -> update -> findOrCreateOrg',
              )
              organizations.push({
                id: orgId,
                source: data.source,
              })
            }
          }

          const emailIdentities = data.identities.filter(
            (i) => i.verified && i.type === MemberIdentityType.EMAIL,
          )
          if (emailIdentities.length > 0) {
            this.log.trace({ memberId: id }, 'Assigning organization by email domain!')
            const orgs = await logExecutionTimeV2(
              () =>
                this.assignOrganizationByEmailDomain(
                  integrationId,
                  emailIdentities.map((i) => i.value),
                ),
              this.log,
              'memberService -> update -> assignOrganizationByEmailDomain',
            )
            if (orgs.length > 0) {
              organizations.push(...orgs)
            }
          }

          if (organizations.length > 0) {
            const uniqOrgs = uniqby(organizations, 'id')
            const orgService = new OrganizationService(this.store, this.log)

            this.log.trace({ memberId: id }, 'Finding member organizations!')
            const orgsToAdd = (
              await Promise.all(
                uniqOrgs.map(async (org) => {
                  // Check if the org was already added to the member in the past, including deleted ones.
                  // If it was, we ignore this org to prevent from adding it again.
                  const existingMemberOrgs = await logExecutionTimeV2(
                    () => orgService.findMemberOrganizations(id, org.id),
                    this.log,
                    'memberService -> update -> findMemberOrganizations',
                  )
                  return existingMemberOrgs.length > 0 ? null : org
                }),
              )
            ).filter((org) => org !== null)

            if (orgsToAdd.length > 0) {
              this.log.trace({ memberId: id }, 'Adding organizations to member!')
              await logExecutionTimeV2(
                () => orgService.addToMember([segmentId], id, orgsToAdd),
                this.log,
                'memberService -> update -> addToMember',
              )
            }
          }
        } catch (err) {
          this.log.error(err, { memberId: id }, 'Error while updating a member!')
          throw err
        }
      },
      this.log,
      'memberService -> update',
    )
  }

  public async assignOrganizationByEmailDomain(
    integrationId: string,
    emails: string[],
  ): Promise<IOrganizationIdSource[]> {
    const orgService = new OrganizationService(this.store, this.log)
    const organizations: IOrganizationIdSource[] = []
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
      const orgId = await orgService.findOrCreate(
        OrganizationAttributeSource.EMAIL,
        integrationId,
        {
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
        },
      )
      if (orgId) {
        organizations.push({
          id: orgId,
          source: orgSource,
        })
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
          'memberService -> update -> getIdentities',
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

  private validateEmails(identities: IMemberIdentity[]): IMemberIdentity[] {
    const toReturn: IMemberIdentity[] = []

    for (const identity of identities) {
      if (identity.type === MemberIdentityType.EMAIL) {
        // check if email is valid and that the same email doesn't already exists in the array for the same platform
        if (
          isEmail(identity.value) &&
          toReturn.find(
            (i) =>
              i.type === MemberIdentityType.EMAIL &&
              i.value === identity.value &&
              i.platform === identity.platform,
          ) === undefined
        ) {
          toReturn.push({ ...identity, value: identity.value.toLowerCase() })
        }
      } else {
        toReturn.push(identity)
      }
    }

    return toReturn
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

      joinedAt = getEarliestValidDate(oldDate, newDate).toISOString()

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
        const dbIdentity = dbIdentities.find(
          (t) =>
            t.platform === identity.platform &&
            t.value === identity.value &&
            t.type === identity.type,
        )
        if (!dbIdentity) {
          newIdentities.push(identity)
        } else if (dbIdentity.verified !== identity.verified) {
          toUpdate.push(identity)
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
}
