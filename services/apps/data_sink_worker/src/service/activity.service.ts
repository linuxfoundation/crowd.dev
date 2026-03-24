import vader from 'crowd-sentiment'
import isEqual from 'lodash.isequal'
import mergeWith from 'lodash.mergewith'

import {
  ApplicationError,
  UnrepeatableError,
  distinct,
  distinctBy,
  escapeNullByte,
  generateUUIDv1,
  isValidEmail,
  parseGitHubNoreplyEmail,
  single,
  singleOrDefault,
  trimUtf8ToMaxByteLength,
} from '@crowd/common'
import { CommonMemberService, SearchSyncWorkerEmitter } from '@crowd/common_services'
import {
  createOrUpdateRelations,
  findIdentitiesForMembers,
  findMembersByIdentities,
  findMembersByVerifiedEmails,
  findMembersByVerifiedUsernames,
  findSegmentsForRepos,
  insertActivities,
  queryActivityRelations,
} from '@crowd/data-access-layer'
import { IDbActivityRelation } from '@crowd/data-access-layer/src/activityRelations/types'
import { DbStore, arePrimitivesDbEqual } from '@crowd/data-access-layer/src/database'
import { getMemberNoMerge } from '@crowd/data-access-layer/src/member_merge'
import {
  IActivityRelationCreateOrUpdateData,
  IDbActivity,
  IDbActivityCreateData,
  IDbActivityUpdateData,
} from '@crowd/data-access-layer/src/old/apps/data_sink_worker/repo/activity.data'
import { IDbMember } from '@crowd/data-access-layer/src/old/apps/data_sink_worker/repo/member.data'
import MemberRepository from '@crowd/data-access-layer/src/old/apps/data_sink_worker/repo/member.repo'
import RequestedForErasureMemberIdentitiesRepository from '@crowd/data-access-layer/src/old/apps/data_sink_worker/repo/requestedForErasureMemberIdentities.repo'
import SettingsRepository from '@crowd/data-access-layer/src/old/apps/data_sink_worker/repo/settings.repo'
import { QueryExecutor, dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { DEFAULT_ACTIVITY_TYPE_SETTINGS, GithubActivityType } from '@crowd/integrations'
import { Logger, LoggerBase, logExecutionTimeV2 } from '@crowd/logging'
import { IQueue } from '@crowd/queue'
import { RedisClient } from '@crowd/redis'
import { Client as TemporalClient } from '@crowd/temporal'
import {
  IActivityData,
  IMemberData,
  IMemberIdentity,
  ISentimentAnalysisResult,
  MemberAttributeName,
  MemberIdentityType,
  PlatformType,
} from '@crowd/types'

import { IActivityUpdateData, ISentimentActivityInput } from './activity.data'
import MemberService from './member.service'
import { IProcessActivityResult } from './types'

/* eslint-disable @typescript-eslint/no-explicit-any */

export default class ActivityService extends LoggerBase {
  private readonly settingsRepo: SettingsRepository
  private readonly memberRepo: MemberRepository
  private readonly commonMemberService: CommonMemberService
  private readonly requestedForErasureMemberIdentitiesRepo: RequestedForErasureMemberIdentitiesRepository

  private readonly pgQx: QueryExecutor

  constructor(
    private readonly pgStore: DbStore,
    private readonly searchSyncWorkerEmitter: SearchSyncWorkerEmitter,
    private readonly redisClient: RedisClient,
    private readonly temporal: TemporalClient,
    private readonly client: IQueue,
    parentLog: Logger,
  ) {
    super(parentLog)

    this.settingsRepo = new SettingsRepository(this.pgStore, this.log)
    this.memberRepo = new MemberRepository(this.pgStore, this.log)
    this.requestedForErasureMemberIdentitiesRepo =
      new RequestedForErasureMemberIdentitiesRepository(this.pgStore, this.log)

    this.pgQx = dbStoreQx(this.pgStore)

    this.commonMemberService = new CommonMemberService(this.pgQx, temporal, this.log)
  }

  public async prepareForUpsert(
    resultId: string,
    segmentId: string,
    timestamp: Date,
    activity: IActivityUpdateData,
    memberInfo: { isBot: boolean; isTeamMember: boolean },
    existingActivityId?: string,
  ): Promise<IActivityPrepareForUpsertResult> {
    // Use the existing activity ID if found, otherwise generate a new one.
    // when existing activityId is passed, tinybird will handle the deduplication
    const id = existingActivityId || generateUUIDv1()

    const sentimentPromise = this.getActivitySentiment({
      body: activity.body,
      title: activity.title,
      type: activity.type,
      platform: activity.platform,
    })

    const payload: IDbActivityCreateData = {
      id,
      timestamp: timestamp.toISOString(),
      platform: activity.platform,
      type: activity.type,
      score: activity.score,
      sourceId: activity.sourceId,
      sourceParentId: activity.sourceParentId,
      memberId: activity.memberId,
      attributes: activity.attributes,
      sentiment: await sentimentPromise,
      title: activity.title,
      body: escapeNullByte(activity.body),
      channel: activity.channel,
      url: activity.url,
      username: activity.username,
      objectMemberId: activity.objectMemberId,
      objectMemberUsername: activity.objectMemberUsername,
      segmentId,
      // if the member is bot, we don't want to affiliate the activity with an organization
      organizationId: memberInfo.isBot ? null : activity.organizationId,
      isBotActivity: memberInfo.isBot,
      isTeamMemberActivity: memberInfo.isTeamMember,
    }

    return {
      resultId,
      activityId: id,
      typeToCreate: activity.type,
      channelToCreate: activity.channel,
      payload,
    }
  }

  private async mergeActivityData(
    data: IActivityUpdateData,
    original: IDbActivity,
  ): Promise<IDbActivityUpdateData> {
    let calcSentiment = false

    let body: string | undefined
    if (!arePrimitivesDbEqual(original.body, data.body)) {
      body = data.body
      calcSentiment = true
    }

    let title: string | undefined
    if (!arePrimitivesDbEqual(original.title, data.title)) {
      title = data.title
      calcSentiment = true
    }

    let sentiment: Promise<ISentimentAnalysisResult | undefined>
    if (calcSentiment) {
      sentiment = this.getActivitySentiment({
        body: body,
        title: title,
        type: original.type,
        platform: original.platform,
      })
    } else {
      sentiment = Promise.resolve(undefined)
    }

    let type: string | undefined
    if (!arePrimitivesDbEqual(original.type, data.type)) {
      type = data.type
    }

    let score: number | undefined
    if (!arePrimitivesDbEqual(original.score, data.score)) {
      score = data.score
    }

    let sourceId: string | undefined
    if (!arePrimitivesDbEqual(original.sourceId, data.sourceId)) {
      sourceId = data.sourceId
    }

    let sourceParentId: string | undefined
    if (!arePrimitivesDbEqual(original.sourceParentId, data.sourceParentId)) {
      sourceParentId = data.sourceParentId
    }

    let memberId: string | undefined
    if (!arePrimitivesDbEqual(original.memberId, data.memberId)) {
      memberId = data.memberId
    }

    let username: string | undefined
    if (!arePrimitivesDbEqual(original.username, data.username)) {
      username = data.username
    }

    let objectMemberId: string | undefined
    if (!arePrimitivesDbEqual(original.objectMemberId, data.objectMemberId)) {
      objectMemberId = data.objectMemberId
    }

    let objectMemberUsername: string | undefined
    if (!arePrimitivesDbEqual(original.objectMemberUsername, data.objectMemberUsername)) {
      objectMemberUsername = data.objectMemberUsername
    }

    let attributes: Record<string, unknown> | undefined
    if (data.attributes && Object.keys(data.attributes).length > 0) {
      const temp = mergeWith({}, original.attributes, data.attributes)

      if (!isEqual(temp, original.attributes)) {
        attributes = temp
      }
    }

    let channel: string | undefined
    if (!arePrimitivesDbEqual(original.channel, data.channel)) {
      channel = data.channel
    }

    let url: string | undefined
    if (!arePrimitivesDbEqual(original.url, data.url)) {
      url = data.url
    }

    let organizationId: string | undefined
    if (!arePrimitivesDbEqual(original.organizationId, data.organizationId)) {
      organizationId = data.organizationId
    }

    let platform: PlatformType | undefined
    if (!arePrimitivesDbEqual(original.platform, data.platform)) {
      platform = data.platform
    }

    return {
      type,
      score,
      sourceId,
      sourceParentId,
      memberId,
      username,
      objectMemberId,
      objectMemberUsername,
      sentiment: await sentiment,
      attributes,
      body,
      title,
      channel,
      url,
      organizationId,
      platform,
    }
  }

  private prepareMemberData(
    data: { resultId: string; activity: IActivityData; platform: PlatformType }[],
  ): Map<string, { success: boolean; err?: Error }> {
    const results = new Map<string, { success: boolean; err?: Error }>()

    for (const { resultId, activity, platform } of data) {
      if (!activity.username && !activity.member) {
        this.log.error({ platform, activity }, 'Activity does not have a username or member.')
        results.set(resultId, {
          success: false,
          err: new UnrepeatableError('Activity does not have a username or member.'),
        })

        continue
      }

      let member = activity.member
      const username = activity.username ? activity.username.trim() : undefined
      if (!member && username) {
        member = {
          identities: [
            {
              platform,
              value: username,
              type: MemberIdentityType.USERNAME,
              verified: true,
              source: 'integration',
            } as IMemberIdentity,
          ],
        }
      }

      member.identities = member.identities.filter((i) => i.value)

      if (!username) {
        const identities = activity.member.identities.filter(
          (i) => i.platform === platform && i.type === MemberIdentityType.USERNAME,
        )

        if (identities.length === 1) {
          activity.username = identities[0].value
        } else if (identities.length === 0) {
          this.log.error(
            { platform, activity },
            `Activity's member does not have an identity for the platform!`,
          )
          results.set(resultId, {
            success: false,
            err: new UnrepeatableError(
              `Activity's member does not have an identity for the platform: ${platform}!`,
            ),
          })

          continue
        } else {
          this.log.error(
            { platform, activity },
            `Activity's member has multiple usernames for the same platform platform!`,
          )
          results.set(resultId, {
            success: false,
            err: new UnrepeatableError(
              `Activity's member has multiple usernames for the same platform: ${platform}!`,
            ),
          })
          continue
        }
      }

      if (!member.attributes) {
        member.attributes = {}
      }

      const objectMemberUsername = activity.objectMemberUsername
        ? activity.objectMemberUsername.trim()
        : undefined
      let objectMember = activity.objectMember

      if (objectMember) {
        objectMember.identities = objectMember.identities.filter((i) => i.value)
      }

      if (objectMember && !objectMemberUsername) {
        const identities = objectMember.identities.filter(
          (i) => i.platform === platform && i.type === MemberIdentityType.USERNAME,
        )

        if (identities.length === 1) {
          activity.objectMemberUsername = identities[0].value
        } else if (identities.length === 0) {
          this.log.error(
            { platform, activity },
            `Activity's  object member does not have an identity for the platform!`,
          )
          results.set(resultId, {
            success: false,
            err: new UnrepeatableError(
              `Activity's object member does not have an identity for the platform: ${platform}!`,
            ),
          })

          continue
        } else {
          this.log.error(
            { platform, activity },
            `Activity's object member has multiple usernames for the same platform platform!`,
          )
          results.set(resultId, {
            success: false,
            err: new UnrepeatableError(
              `Activity's object member has multiple usernames for the same platform: ${platform}!`,
            ),
          })
          continue
        }
      } else if (objectMemberUsername && !objectMember) {
        objectMember = {
          identities: [
            {
              platform,
              value: objectMemberUsername,
              type: MemberIdentityType.USERNAME,
              verified: true,
              source: 'integration',
            } as IMemberIdentity,
          ],
        }
      }

      results.set(resultId, { success: true })
    }

    return results
  }

  public async processActivities(
    payloads: IActivityProcessData[],
    onboarding: boolean,
  ): Promise<Map<string, IProcessActivityResult>> {
    const resultMap = new Map<string, IProcessActivityResult>()

    let relevantPayloads = payloads
    this.log.trace(`[ACTIVITY] Processing ${relevantPayloads.length} activities!`)

    const prepareMemberResults = this.prepareMemberData(relevantPayloads)

    relevantPayloads = []
    for (const [resultId, { success, err }] of prepareMemberResults) {
      if (!success) {
        resultMap.set(resultId, { success: false, err })
      } else {
        relevantPayloads.push(single(payloads, (a) => a.resultId === resultId))
      }
    }

    if (relevantPayloads.length === 0) {
      return resultMap
    }

    this.log.trace(
      `[ACTIVITY] We still have ${relevantPayloads.length} activities left to process after member preparation!`,
    )

    const allMemberIdentities = relevantPayloads
      .flatMap((a) => a.activity.member.identities)
      .concat(
        relevantPayloads
          .filter((a) => a.activity.objectMember)
          .flatMap((a) => a.activity.objectMember.identities),
      )

    // handle identities that were requested to be erased by the user
    const toErase = await logExecutionTimeV2(
      async () =>
        this.requestedForErasureMemberIdentitiesRepo.someIdentitiesWereErasedByUserRequest(
          allMemberIdentities,
        ),
      this.log,
      'processActivities -> someIdentitiesWereErasedByUserRequest',
    )

    const handleErasure = (member: IMemberData, resultId: string): boolean => {
      const toEraseMemberIdentities = toErase.filter((e) =>
        member.identities.some((i) => {
          if (i.type === MemberIdentityType.EMAIL) {
            return e.type === i.type && e.value === i.value
          }

          return e.type === i.type && e.value === i.value && e.platform === i.platform
        }),
      )

      if (toEraseMemberIdentities.length > 0) {
        if (toEraseMemberIdentities.some((i) => i.verified)) {
          this.log.warn(
            {
              memberIdentities: member.identities,
            },
            'Member has identities that were requested to be erased by the user! Skipping activity processing!',
          )
          // set result to true cuz it was processed and we don't need to store error
          resultMap.set(resultId, { success: true })
          // remove activity from relevant activities because it's not valid anymore
          relevantPayloads = relevantPayloads.filter((a) => a.resultId !== resultId)

          return false
        } else {
          // remove unverified identities that were marked to be erased so they are not created
          member.identities = member.identities.filter((i) => {
            if (i.verified) return true

            const maybeToErase = toEraseMemberIdentities.find(
              (e) =>
                e.type === i.type &&
                e.value === i.value &&
                (e.type === MemberIdentityType.EMAIL || e.platform === i.platform),
            )

            if (maybeToErase) return false
            return true
          })

          if (member.identities.filter((i) => i.value).length === 0) {
            this.log.warn(
              'Member had at least one unverified identity removed as it was requested to be removed! Now there is no identities left - skipping processing!',
            )

            // set result map to true cuz it was processed and we don't need to store error
            resultMap.set(resultId, { success: true })
            // remove activity from relevant activities because it's not valid anymore
            relevantPayloads = relevantPayloads.filter((a) => a.resultId !== resultId)

            return false
          }
        }
      }

      return true
    }

    let promises = []

    const repoPayloads: IActivityProcessData[] = []
    for (const payload of relevantPayloads) {
      if (!handleErasure(payload.activity.member, payload.resultId)) {
        continue
      }

      if (
        payload.activity.objectMember &&
        !handleErasure(payload.activity.objectMember, payload.resultId)
      ) {
        continue
      }

      if (payload.platform === PlatformType.GITLAB || payload.platform === PlatformType.GITHUB) {
        repoPayloads.push(payload)
      } else if (!payload.segmentId) {
        resultMap.set(payload.resultId, {
          success: false,
          err: new UnrepeatableError(
            'No segmentId provided! Something went wrong - it should be set in the result data or taken from integrations.segmentId column!',
          ),
        })
        relevantPayloads = relevantPayloads.filter((a) => a.resultId !== payload.resultId)
      }
    }

    // determine segmentIds from public.repositories
    const distinctChannels = distinctBy(
      repoPayloads,
      (a) => `${a.integrationId}-${a.activity.channel}`,
    )

    if (distinctChannels.length > 0) {
      this.log.info(
        { repoPayloads: repoPayloads.length, distinctChannels: distinctChannels.length },
        '[ACTIVITY] Looking up segments from public.repositories',
      )

      promises.push(
        findSegmentsForRepos(
          this.pgQx,
          this.redisClient,
          this.log,
          distinctChannels.map((c) => ({
            integrationId: c.integrationId,
            url: c.activity.channel,
          })),
        ).then((results) => {
          for (const result of results) {
            if (result.segmentId) {
              for (const payload of repoPayloads.filter(
                (p) =>
                  p.integrationId === result.integrationId && p.activity.channel === result.url,
              )) {
                payload.segmentId = result.segmentId
              }
            }
          }
        }),
      )
    }

    await Promise.all(promises)

    this.log.trace(
      `[ACTIVITY] We still have ${relevantPayloads.length} activities left to process after finding segments!`,
    )

    const orConditions = relevantPayloads.map((r) => {
      return {
        and: [
          { timestamp: { eq: r.activity.timestamp } },
          { sourceId: { eq: r.activity.sourceId } },
          { platform: { eq: r.activity.platform } },
          { type: { eq: r.activity.type } },
          { channel: { eq: r.activity.channel } },
        ],
      }
    })

    const segmentIds = distinct(relevantPayloads.map((r) => r.segmentId))

    // Check activityRelations to find existence
    // If found, we reuse the activityId and let tinybird handle the upsert via DEDUP keys.
    // This avoids querying tinybird and merging data, simplifying the logic and making it
    // more resilient to data replication delays.
    const existingActivityRelations = await logExecutionTimeV2(
      async () =>
        queryActivityRelations(
          this.pgQx,
          {
            segmentIds,
            filter: {
              and: [
                {
                  timestamp: {
                    in: distinct(relevantPayloads.map((r) => r.activity.timestamp)),
                  },
                },
                {
                  or: orConditions,
                },
              ],
            },
            limit: relevantPayloads.length,
            noCount: true,
          },
          [
            'activityId',
            'timestamp',
            'memberId',
            'objectMemberId',
            'organizationId',
            'conversationId',
            'parentId',
            'type',
            'sourceId',
            'sourceParentId',
            'channel',
            'segmentId',
            'platform',
            'username',
            'objectMemberUsername',
            'sentimentScore',
            'gitInsertions',
            'gitDeletions',
            'score',
            'pullRequestReviewState',
          ],
        ),
      this.log,
      'processActivities -> queryActivityRelations',
    )

    // map existing activities to payloads for further processing
    const payloadsNotInDb: IActivityProcessData[] = []
    for (const payload of relevantPayloads) {
      const existingRelation = singleOrDefault(existingActivityRelations.rows, (a) => {
        if (a.segmentId !== payload.segmentId) {
          return false
        }

        if (a.platform !== payload.platform) {
          return false
        }

        if (a.type !== payload.activity.type) {
          return false
        }

        if (a.sourceId !== payload.activity.sourceId) {
          return false
        }

        if (payload.activity.channel) {
          if (a.channel !== payload.activity.channel) {
            return false
          }
        }

        const aTimestamp = new Date(a.timestamp).toISOString()
        const pTimestamp = new Date(payload.activity.timestamp).toISOString()
        return aTimestamp === pTimestamp
      })

      if (existingRelation) {
        payload.activityId = existingRelation.activityId
        payload.dbActivityRelation = existingRelation
      }

      // Regardless of whether the activity already exists, we always resolve the
      // owning member from identities (username/email/etc.) instead of trusting
      // the existing relation.memberId. This ensures activities always follow
      // the current owner of the identity.
      payloadsNotInDb.push(payload)
    }

    if (payloadsNotInDb.length > 0) {
      // map DB results to payloads by matching platform + value
      const mapResultsToPayloads = (
        results: Map<string | { platform: string; value: string }, any>,
        matchFn: (p: any, value: string, platform?: string) => boolean,
        setMemberFn: (p: any, member: any) => void,
      ) => {
        for (const [key, dbMember] of results) {
          const value = typeof key === 'string' ? key : key.value
          const platform = typeof key === 'string' ? undefined : key.platform

          payloadsNotInDb
            .filter((p) => matchFn(p, value, platform))
            .forEach((p) => setMemberFn(p, dbMember))
        }
      }

      // Look up members using verified usernames (same platform)
      const usernameFilter = payloadsNotInDb
        .filter((p) => !p.dbMember)
        .map((p) => ({
          platform: p.platform,
          username: p.activity.username,
          segmentId: p.segmentId,
        }))
        .concat(
          payloadsNotInDb
            .filter((p) => !p.dbObjectMember && p.activity.objectMemberUsername)
            .map((p) => ({
              platform: p.platform,
              username: p.activity.objectMemberUsername,
              segmentId: p.segmentId,
            })),
        )

      if (usernameFilter.length > 0) {
        const dbMembersByUsername = await logExecutionTimeV2(
          async () => findMembersByVerifiedUsernames(this.pgQx, usernameFilter),
          this.log,
          'processActivities -> memberRepo.findMembersByUsernames',
        )

        mapResultsToPayloads(
          dbMembersByUsername,
          (p, value, platform) =>
            !p.dbMember &&
            p.platform === platform &&
            p.activity.username?.toLowerCase() === value.toLowerCase(),
          (p, member) => {
            p.dbMember = member
            p.dbMemberSource = 'username'
          },
        )

        mapResultsToPayloads(
          dbMembersByUsername,
          (p, value, platform) =>
            !p.dbObjectMember &&
            p.platform === platform &&
            p.activity.objectMemberUsername?.toLowerCase() === value.toLowerCase(),
          (p, member) => {
            p.dbObjectMember = member
            p.dbObjectMemberSource = 'username'
          },
        )
      }

      // Look up members using verified emails (same platform)
      const emails = new Set<string>()
      for (const payload of payloadsNotInDb.filter((p) => !p.dbMember)) {
        for (const identity of payload.activity.member.identities.filter(
          (i) => i.verified && i.type === MemberIdentityType.EMAIL,
        )) {
          emails.add(identity.value)
        }
      }
      for (const payload of payloadsNotInDb.filter(
        (p) => !p.dbObjectMember && p.activity.objectMember,
      )) {
        for (const identity of payload.activity.objectMember.identities.filter(
          (i) => i.verified && i.type === MemberIdentityType.EMAIL,
        )) {
          emails.add(identity.value)
        }
      }

      if (emails.size > 0) {
        const dbMembersByEmail = await logExecutionTimeV2(
          () => findMembersByVerifiedEmails(this.pgQx, Array.from(emails)),
          this.log,
          'processActivities -> memberRepo.findMembersByEmails',
        )

        mapResultsToPayloads(
          dbMembersByEmail,
          (p, value) =>
            !p.dbMember &&
            p.activity.member.identities.some(
              (i) =>
                i.verified &&
                i.type === MemberIdentityType.EMAIL &&
                i.value.toLowerCase() === value.toLowerCase(),
            ),
          (p, member) => {
            p.dbMember = member
            p.dbMemberSource = 'email'
          },
        )

        mapResultsToPayloads(
          dbMembersByEmail,
          (p, value) =>
            !p.dbObjectMember &&
            p.activity.objectMember?.identities.some(
              (i) =>
                i.verified &&
                i.type === MemberIdentityType.EMAIL &&
                i.value.toLowerCase() === value.toLowerCase(),
            ),
          (p, member) => {
            p.dbObjectMember = member
            p.dbObjectMemberSource = 'email'
          },
        )
      }

      // Look up members by parsing noreply emails to extract platform usernames
      // e.g. "123+john@users.noreply.github.com" -> GitHub username "john"
      const noreplyEmailFilterMap = new Map<
        string,
        { platform: PlatformType; username: string; segmentId: string }
      >()

      for (const payload of payloadsNotInDb.filter((p) => !p.dbMember)) {
        for (const identity of payload.activity.member.identities.filter(
          (i) => i.verified && i.type === MemberIdentityType.EMAIL,
        )) {
          const ghUsername = parseGitHubNoreplyEmail(identity.value)
          if (ghUsername) {
            const key = `${PlatformType.GITHUB}:${ghUsername}:${payload.segmentId}`
            if (!noreplyEmailFilterMap.has(key)) {
              noreplyEmailFilterMap.set(key, {
                platform: PlatformType.GITHUB,
                username: ghUsername,
                segmentId: payload.segmentId,
              })
            }
          }
        }
      }

      for (const payload of payloadsNotInDb.filter(
        (p) => !p.dbObjectMember && p.activity.objectMember,
      )) {
        for (const identity of payload.activity.objectMember.identities.filter(
          (i) => i.verified && i.type === MemberIdentityType.EMAIL,
        )) {
          const ghUsername = parseGitHubNoreplyEmail(identity.value)
          if (ghUsername) {
            const key = `${PlatformType.GITHUB}:${ghUsername}:${payload.segmentId}`
            if (!noreplyEmailFilterMap.has(key)) {
              noreplyEmailFilterMap.set(key, {
                platform: PlatformType.GITHUB,
                username: ghUsername,
                segmentId: payload.segmentId,
              })
            }
          }
        }
      }

      if (noreplyEmailFilterMap.size > 0) {
        const dbMembersByNoreplyEmail = await logExecutionTimeV2(
          async () =>
            findMembersByVerifiedUsernames(this.pgQx, Array.from(noreplyEmailFilterMap.values())),
          this.log,
          'processActivities -> memberRepo.findMembersByVerifiedUsernames (noreply-email)',
        )

        mapResultsToPayloads(
          dbMembersByNoreplyEmail,
          (p, value) =>
            !p.dbMember &&
            p.activity.member.identities.some(
              (i) =>
                i.verified &&
                i.type === MemberIdentityType.EMAIL &&
                parseGitHubNoreplyEmail(i.value) === value.toLowerCase(),
            ),
          (p, member) => {
            p.dbMember = member
            p.dbMemberSource = 'email'
          },
        )

        mapResultsToPayloads(
          dbMembersByNoreplyEmail,
          (p, value) =>
            !p.dbObjectMember &&
            p.activity.objectMember?.identities.some(
              (i) =>
                i.verified &&
                i.type === MemberIdentityType.EMAIL &&
                parseGitHubNoreplyEmail(i.value) === value.toLowerCase(),
            ),
          (p, member) => {
            p.dbObjectMember = member
            p.dbObjectMemberSource = 'email'
          },
        )
      }

      // Look up members using cross-identity matching (different platforms)
      // we will check only on platforms that store email identities as usernames

      // only these platforms are considered for emails-as-usernames
      const EMAIL_AS_USERNAME_PLATFORMS: PlatformType[] = [
        PlatformType.GERRIT,
        PlatformType.JIRA,
        PlatformType.CONFLUENCE,
      ]

      // Verified email identities -> match to verified usernames
      const emailAsUsernameFilter = payloadsNotInDb
        .filter(
          (p) =>
            !p.dbMember &&
            p.activity.username &&
            p.activity.member.identities.some(
              (i) => i.verified && i.type === MemberIdentityType.EMAIL,
            ),
        )
        .flatMap((p) =>
          EMAIL_AS_USERNAME_PLATFORMS.map((platform) => ({
            platform,
            username: p.activity.username,
            segmentId: p.segmentId,
          })),
        )
        .concat(
          payloadsNotInDb
            .filter(
              (p) =>
                !p.dbObjectMember &&
                p.activity.objectMember &&
                p.activity.objectMemberUsername &&
                p.activity.objectMember.identities.some(
                  (i) => i.verified && i.type === MemberIdentityType.EMAIL,
                ),
            )
            .flatMap((p) =>
              EMAIL_AS_USERNAME_PLATFORMS.map((platform) => ({
                platform,
                username: p.activity.objectMemberUsername,
                segmentId: p.segmentId,
              })),
            ),
        )

      if (emailAsUsernameFilter.length > 0) {
        const dbMembersByEmailAsUsername = await logExecutionTimeV2(
          async () => findMembersByVerifiedUsernames(this.pgQx, emailAsUsernameFilter),
          this.log,
          'processActivities -> memberRepo.findMembersByVerifiedUsernames (email-as-username)',
        )

        mapResultsToPayloads(
          dbMembersByEmailAsUsername,
          (p, value) =>
            !p.dbMember &&
            p.activity.member.identities.some(
              (i) =>
                i.verified &&
                i.type === MemberIdentityType.EMAIL &&
                i.value.toLowerCase() === value.toLowerCase(),
            ),
          (p, member) => {
            p.dbMember = member
            p.dbMemberSource = 'username'
          },
        )

        mapResultsToPayloads(
          dbMembersByEmailAsUsername,
          (p, value) =>
            !p.dbObjectMember &&
            p.activity.objectMember?.identities.some(
              (i) =>
                i.verified &&
                i.type === MemberIdentityType.EMAIL &&
                i.value.toLowerCase() === value.toLowerCase(),
            ),
          (p, member) => {
            p.dbObjectMember = member
            p.dbObjectMemberSource = 'username'
          },
        )
      }

      // Verified email-like usernames -> match to verified email identities
      const emailLikeUsernames = new Set<string>()
      for (const payload of payloadsNotInDb.filter((p) => !p.dbMember)) {
        for (const identity of payload.activity.member.identities.filter(
          (i) => i.verified && i.type === MemberIdentityType.USERNAME && isValidEmail(i.value),
        )) {
          emailLikeUsernames.add(identity.value)
        }
      }

      for (const payload of payloadsNotInDb.filter(
        (p) => !p.dbObjectMember && p.activity.objectMember,
      )) {
        for (const identity of payload.activity.objectMember.identities.filter(
          (i) => i.verified && i.type === MemberIdentityType.USERNAME && isValidEmail(i.value),
        )) {
          emailLikeUsernames.add(identity.value)
        }
      }

      if (emailLikeUsernames.size > 0) {
        const dbMembersByEmailLikeUsername = await logExecutionTimeV2(
          async () => findMembersByVerifiedEmails(this.pgQx, Array.from(emailLikeUsernames)),
          this.log,
          'processActivities -> memberRepo.findMembersByVerifiedEmails (email-like username)',
        )

        mapResultsToPayloads(
          dbMembersByEmailLikeUsername,
          (p, value) =>
            !p.dbMember &&
            p.activity.member.identities.some(
              (i) =>
                i.verified &&
                i.type === MemberIdentityType.USERNAME &&
                isValidEmail(i.value) &&
                i.value.toLowerCase() === value.toLowerCase(),
            ),
          (p, member) => {
            p.dbMember = member
            p.dbMemberSource = 'email'
          },
        )

        mapResultsToPayloads(
          dbMembersByEmailLikeUsername,
          (p, value) =>
            !p.dbObjectMember &&
            p.activity.objectMember?.identities.some(
              (i) =>
                i.verified &&
                i.type === MemberIdentityType.USERNAME &&
                isValidEmail(i.value) &&
                i.value.toLowerCase() === value.toLowerCase(),
            ),
          (p, member) => {
            p.dbObjectMember = member
            p.dbObjectMemberSource = 'email'
          },
        )
      }
    }

    // we should have now all relevant dbActivity, dbMember and dbObjectMember set
    // we can now upsert activities and members
    this.log.trace(
      `[ACTIVITY] We still have ${relevantPayloads.length} activities left after mapping db activities and members!`,
    )
    const preparedActivities: IActivityPrepareForUpsertResult[] = []

    const memberService = new MemberService(this.pgStore, this.redisClient, this.temporal, this.log)

    // find distinct members to create
    const payloadsWithoutDbMembers: IActivityProcessData[] = relevantPayloads.filter(
      (p) => !p.dbMember,
    )
    const payloadsWithoutDbObjectMembers: IActivityProcessData[] = relevantPayloads.filter(
      (p) => p.activity.objectMember && !p.dbObjectMember,
    )

    // gather all the data we need to create members
    // key: platform:username
    const membersToCreateMap = new Map<
      string,
      {
        member: IMemberData
        segmentIds: Set<string>
        resultIds: Set<string>
        integrationId: string
        platform: string
        username: string
        timestamp: string
      }
    >()

    // find members to create
    for (const payload of payloadsWithoutDbMembers) {
      const key = `${payload.platform}:${payload.activity.username}`
      if (!membersToCreateMap.has(key)) {
        const segmentIds = new Set<string>()
        segmentIds.add(payload.segmentId)

        const resultIds = new Set<string>()
        resultIds.add(payload.resultId)

        membersToCreateMap.set(key, {
          member: payload.activity.member,
          integrationId: payload.integrationId,
          platform: payload.platform,
          username: payload.activity.username,
          timestamp: payload.activity.timestamp,
          segmentIds,
          resultIds,
        })
      } else {
        const value = membersToCreateMap.get(key)
        value.segmentIds.add(payload.segmentId)
        value.resultIds.add(payload.resultId)
      }
    }

    // find object members to create
    for (const payload of payloadsWithoutDbObjectMembers) {
      const key = `${payload.platform}:${payload.activity.objectMemberUsername}`
      if (!membersToCreateMap.has(key)) {
        const segmentIds = new Set<string>()
        segmentIds.add(payload.segmentId)

        const resultIds = new Set<string>()
        resultIds.add(payload.resultId)

        membersToCreateMap.set(key, {
          member: payload.activity.member,
          integrationId: payload.integrationId,
          platform: payload.platform,
          username: payload.activity.objectMemberUsername,
          timestamp: payload.activity.timestamp,
          segmentIds,
          resultIds,
        })
      } else {
        const value = membersToCreateMap.get(key)
        value.segmentIds.add(payload.segmentId)
        value.resultIds.add(payload.resultId)
      }
    }

    // clear the promises array - should be all completed by now
    promises = []
    for (const value of membersToCreateMap.values()) {
      promises.push(
        memberService
          .create(
            Array.from(value.segmentIds),
            value.integrationId,
            {
              displayName: value.member.displayName || value.username,
              attributes: value.member.attributes,
              joinedAt: value.member.joinedAt
                ? new Date(value.member.joinedAt)
                : new Date(value.timestamp),
              identities: value.member.identities,
              organizations: value.member.organizations,
              reach: value.member.reach,
            },
            value.platform,
          )
          .then((memberId) => {
            // map ids for members
            for (const payload of relevantPayloads.filter(
              (p) =>
                !p.dbMember &&
                p.platform === value.platform &&
                p.activity.username === value.username,
            )) {
              payload.memberId = memberId
            }

            // map ids for object members
            for (const payload of relevantPayloads.filter(
              (p) =>
                p.activity.objectMember &&
                !p.dbObjectMember &&
                p.platform === value.platform &&
                p.activity.objectMemberUsername === value.username,
            )) {
              payload.objectMemberId = memberId
            }
          })
          .catch(async (err) => {
            // to have all the merged members in one place
            const memberMap = new Map<string, string>()
            for (const resultId of value.resultIds) {
              const payload = single(relevantPayloads, (p) => p.resultId === resultId)

              if (
                payload.platform === value.platform &&
                payload.activity.username === value.username
              ) {
                const key = `${payload.platform}:${payload.activity.username}`
                if (memberMap.has(key)) {
                  payload.memberId = memberMap.get(key)
                } else {
                  const result = await this.handleMemberIdentityError(err, payload, 'member')
                  if (result) {
                    if (typeof result === 'string') {
                      // we have a merged member id
                      payload.memberId = result
                      memberMap.set(key, result)
                    } else {
                      resultMap.set(resultId, {
                        success: false,
                        err: new ApplicationError('Error while creating member!', err),
                        metadata: result,
                      })
                    }
                  } else {
                    resultMap.set(resultId, {
                      success: false,
                      err: new ApplicationError('Error while creating member!', err),
                    })
                  }
                }
              } else if (
                payload.platform === value.platform &&
                payload.activity.objectMemberUsername == value.username
              ) {
                const key = `${payload.platform}:${payload.activity.objectMemberUsername}`
                if (memberMap.has(key)) {
                  payload.objectMemberId = memberMap.get(key)
                } else {
                  const result = await this.handleMemberIdentityError(err, payload, 'objectMember')

                  if (result) {
                    if (typeof result === 'string') {
                      // we have a merged member id
                      payload.objectMemberId = result
                      memberMap.set(key, result)
                    } else {
                      resultMap.set(resultId, {
                        success: false,
                        err: new ApplicationError('Error while creating object member!', err),
                        metadata: result,
                      })
                    }
                  } else {
                    resultMap.set(resultId, {
                      success: false,
                      err: new ApplicationError('Error while creating object member!', err),
                    })
                  }
                }
              } else {
                resultMap.set(resultId, {
                  success: false,
                  err: new ApplicationError('Error while creating unknown member!', err),
                })
              }
            }
          }),
      )
    }
    await Promise.all(promises)

    const memberIds = new Set<string>()
    for (const payload of relevantPayloads) {
      if (payload.dbMember) {
        memberIds.add(payload.dbMember.id)
      }

      if (payload.dbObjectMember) {
        memberIds.add(payload.dbObjectMember.id)
      }
    }

    let dbMemberIdentities: Map<string, IMemberIdentity[]> = new Map()
    if (memberIds.size > 0) {
      dbMemberIdentities = await findIdentitiesForMembers(this.pgQx, Array.from(memberIds))
    }

    for (const payload of relevantPayloads) {
      // contains the merged member ids
      const memberMap = new Map<string, string>()

      const promises = []
      // update members and orgs with them
      if (payload.dbMember) {
        const key = `${payload.platform}:${payload.activity.username}`
        if (memberMap.has(key)) {
          payload.memberId = memberMap.get(key)
        } else {
          promises.push(
            memberService
              .update(
                payload.dbMember.id,
                payload.segmentId,
                payload.integrationId,
                {
                  attributes: payload.activity.member.attributes,
                  joinedAt: payload.activity.member.joinedAt
                    ? new Date(payload.activity.member.joinedAt)
                    : new Date(payload.activity.timestamp),
                  identities: payload.activity.member.identities,
                  organizations: payload.activity.member.organizations,
                  reach: payload.activity.member.reach,
                },
                payload.dbMember,
                dbMemberIdentities.get(payload.dbMember.id),
                payload.platform,
              )
              .then(() => {
                payload.memberId = payload.dbMember.id
              })
              .catch(async (err) => {
                const result = await this.handleMemberIdentityError(
                  err,
                  payload,
                  'member',
                  payload.dbMember,
                )
                if (result) {
                  if (typeof result === 'string') {
                    payload.memberId = result
                    memberMap.set(key, result)
                  } else {
                    resultMap.set(payload.resultId, {
                      success: false,
                      err: new ApplicationError('Error while updating member!', err),
                      metadata: result,
                    })
                  }
                } else {
                  resultMap.set(payload.resultId, {
                    success: false,
                    err: new ApplicationError('Error while updating member!', err),
                  })
                }
              }),
          )
        }
      }

      if (payload.dbObjectMember) {
        const key = `${payload.platform}:${payload.activity.objectMemberUsername}`
        if (memberMap.has(key)) {
          payload.objectMemberId = memberMap.get(key)
        } else {
          promises.push(
            memberService
              .update(
                payload.dbObjectMember.id,
                payload.segmentId,
                payload.integrationId,
                {
                  attributes: payload.activity.objectMember.attributes,
                  joinedAt: payload.activity.objectMember.joinedAt
                    ? new Date(payload.activity.objectMember.joinedAt)
                    : new Date(payload.activity.timestamp),
                  identities: payload.activity.objectMember.identities,
                  organizations: payload.activity.objectMember.organizations,
                  reach: payload.activity.objectMember.reach,
                },
                payload.dbObjectMember,
                dbMemberIdentities.get(payload.dbObjectMember.id),
                payload.platform,
              )
              .then(() => {
                payload.objectMemberId = payload.dbObjectMember.id
              })
              .catch(async (err) => {
                const result = await this.handleMemberIdentityError(
                  err,
                  payload,
                  'objectMember',
                  payload.dbObjectMember,
                )
                if (result) {
                  if (typeof result === 'string') {
                    payload.objectMemberId = result
                    memberMap.set(key, result)
                  } else {
                    resultMap.set(payload.resultId, {
                      success: false,
                      err: new ApplicationError('Error while updating object member!', err),
                      metadata: result,
                    })
                  }
                } else {
                  resultMap.set(payload.resultId, {
                    success: false,
                    err: new ApplicationError('Error while updating object member!', err),
                  })
                }
              }),
          )
        }
      }

      await Promise.all(promises)

      if (resultMap.has(payload.resultId)) {
        continue
      }

      const isBot = this.memberAttValue(
        MemberAttributeName.IS_BOT,
        payload.activity.member,
        payload.platform,
        payload.dbMember,
      ) as boolean

      if (!isBot) {
        // associate activity with organization
        payload.organizationId = await this.commonMemberService.findAffiliation(
          payload.memberId,
          payload.segmentId,
          payload.activity.timestamp,
        )
      } else {
        // for bot members, we don't want to affiliate the activity with an organization
        payload.organizationId = null
        this.log.trace(
          { memberId: payload.memberId },
          'Skipping organization affiliation for bot member!',
        )
      }

      if (!payload.memberId) {
        this.log.error(`Member id not set - can't continue!`)
        throw new Error(`Member id not set - can't continue!`)
      }

      if (payload.activity.objectMember && !payload.objectMemberId) {
        this.log.error(`Object member id not set - can't continue!`)
        throw new Error(`Object member id not set - can't continue!`)
      }

      preparedActivities.push(
        await this.prepareForUpsert(
          payload.resultId,
          payload.segmentId,
          new Date(payload.activity.timestamp),
          {
            type: payload.activity.type,
            platform: payload.platform,
            sourceId: payload.activity.sourceId,
            score: payload.activity.score,
            sourceParentId:
              payload.platform === PlatformType.GITHUB &&
              payload.activity.type === GithubActivityType.AUTHORED_COMMIT &&
              payload.activity.sourceParentId
                ? undefined
                : payload.activity.sourceParentId,
            memberId: payload.memberId,
            username: payload.activity.username,
            objectMemberId: payload.objectMemberId,
            objectMemberUsername: payload.activity.objectMemberUsername,
            attributes: payload.activity.attributes || {},
            body: payload.activity.body,
            title: payload.activity.title,
            channel: payload.activity.channel,
            url: payload.activity.url,
            organizationId: payload.organizationId,
          },
          {
            isBot,
            isTeamMember: this.memberAttValue(
              MemberAttributeName.IS_TEAM_MEMBER,
              payload.activity.member,
              payload.platform,
              payload.dbMember,
            ) as boolean,
          },
          payload.activityId,
        ),
      )
    }

    this.log.trace(`[ACTIVITY] We have ${preparedActivities.length} intermediate results!`)

    const activitiesWithPayload = preparedActivities.filter((a) => a.payload)

    const uniqueConstraintKeys = new Set<string>()
    const preparedForUpsert = []

    for (const item of activitiesWithPayload) {
      // deduplication key with placeholders for empty values
      const key = [
        item.payload.timestamp,
        item.payload.platform,
        item.payload.type,
        item.payload.sourceId,
        item.payload.channel,
        item.payload.segmentId,
      ]
        .map((v) => (v !== undefined && v !== null && v !== '' ? v : '<empty>'))
        .join('|')

      if (!uniqueConstraintKeys.has(key)) {
        uniqueConstraintKeys.add(key)
        preparedForUpsert.push(item)
      }
    }

    const toUpsert = preparedForUpsert.map((a) => a.payload)
    if (toUpsert.length > 0) {
      this.log.trace(`[ACTIVITY] Upserting ${toUpsert.length} activities!`)
      await insertActivities(this.client, toUpsert)
    }

    // Filter out relations that don't need updates to avoid unnecessary database writes
    let skippedCount = 0
    const relationsToUpsert = preparedForUpsert
      .map((a) => {
        const relationData: IActivityRelationCreateOrUpdateData = {
          activityId: a.payload.id,
          segmentId: a.payload.segmentId,
          memberId: a.payload.memberId,
          objectMemberId: a.payload.objectMemberId,
          organizationId: a.payload.organizationId,
          conversationId: a.payload.conversationId,
          platform: a.payload.platform,
          username: a.payload.username,
          objectMemberUsername: a.payload.objectMemberUsername,
          parentId: a.payload.parentId,
          sourceId: a.payload.sourceId,
          sourceParentId: a.payload.sourceParentId,
          type: a.payload.type,
          timestamp: a.payload.timestamp,
          channel: a.payload.channel,
          sentimentScore: a.payload.sentimentScore,
          gitInsertions: a.payload.gitInsertions,
          gitDeletions: a.payload.gitDeletions,
          score: a.payload.score,
          pullRequestReviewState: a.payload.attributes?.reviewState as string,
        }

        return {
          relation: relationData,
          activity: a,
        }
      })
      .filter((data) => {
        // If we have an existing relation, check if it actually needs updating
        if (data.activity.dbActivityRelation) {
          if (!this.needsActivityRelationUpdate(data.activity.dbActivityRelation, data.relation)) {
            skippedCount++
            return false
          }
        }

        return true
      })
      .map((d) => d.relation)

    if (relationsToUpsert.length > 0) {
      this.log.trace(
        `[ACTIVITY] Upserting ${relationsToUpsert.length} activity relations (filtered from ${preparedForUpsert.length}, skipped ${skippedCount})`,
      )
      await createOrUpdateRelations(this.pgQx, relationsToUpsert)
    } else {
      this.log.trace(
        `[ACTIVITY] No activity relations need updating (all ${preparedForUpsert.length} would only update updatedAt)`,
      )
    }

    const createdTypes = new Set<string>()
    const createdChannels = new Set<string>()
    for (const prepared of preparedForUpsert) {
      if (
        prepared.typeToCreate &&
        !createdTypes.has(
          `${prepared.typeToCreate}:${prepared.payload.platform}:${prepared.payload.segmentId}`,
        )
      ) {
        await this.settingsRepo.createActivityType(
          prepared.payload.platform as PlatformType,
          prepared.typeToCreate,
          prepared.payload.segmentId,
        )

        createdTypes.add(
          `${prepared.typeToCreate}:${prepared.payload.platform}:${prepared.payload.segmentId}`,
        )
      }
      if (
        prepared.channelToCreate &&
        !createdChannels.has(
          `${prepared.payload.platform}:${prepared.payload.channel}:${prepared.payload.segmentId}`,
        )
      ) {
        await this.settingsRepo.createActivityChannel(
          prepared.payload.segmentId,
          prepared.payload.platform,
          prepared.channelToCreate,
        )

        createdChannels.add(
          `${prepared.payload.platform}:${prepared.payload.channel}:${prepared.payload.segmentId}`,
        )
      }

      await this.searchSyncWorkerEmitter.triggerMemberSync(
        prepared.payload.memberId,
        onboarding,
        prepared.payload.segmentId,
      )

      if (prepared.payload.objectMemberId) {
        await this.searchSyncWorkerEmitter.triggerMemberSync(
          prepared.payload.objectMemberId,
          onboarding,
          prepared.payload.segmentId,
        )
      }

      if (prepared.payload.organizationId) {
        await this.redisClient.sAdd(
          'organizationIdsForAggComputation',
          prepared.payload.organizationId,
        )
      }
    }

    for (const prepared of preparedActivities) {
      resultMap.set(prepared.resultId, { success: true })
    }

    this.log.trace(`[ACTIVITY] We have ${resultMap.size} results to return!`)
    return resultMap
  }

  private async handleMemberIdentityError(
    error: any,
    payload: IActivityProcessData,
    memberType: 'member' | 'objectMember',
    dbMember?: IDbMember,
  ): Promise<string | Record<string, unknown> | undefined> {
    const checkForIdentityConstraint = (error: any): boolean => {
      if (
        error.constructor &&
        error.constructor.name === 'DatabaseError' &&
        error.constraint &&
        error.constraint === 'uix_memberIdentities_platform_value_type_verified' &&
        error.detail
      ) {
        return true
      }

      return false
    }

    const extractMetadata = async (
      error: any,
    ): Promise<string | Record<string, unknown> | undefined> => {
      const metadata: Record<string, unknown> = {}

      // extract the platform, value, type from the detail
      const detail = error.detail
      const regex = /\(platform, value, type\)=\((.*?)\)/
      const match = detail.match(regex)

      if (!match || match.length < 2) {
        return
      }

      // Split the matched string by commas
      const values = match[1].split(',').map((val) => val.trim())

      // Extract platform, value, and type
      const [platform, value, type] = values

      metadata.erroredVerifiedIdentity = {
        platform,
        value,
        type,
      }

      const membersWithIdentity = await findMembersByIdentities(
        this.pgQx,
        [
          {
            platform,
            value,
            type,
            verified: true,
          } as IMemberIdentity,
        ],
        undefined,
        true,
      )

      if (memberType === 'member') {
        metadata.verifiedIdentities = payload.activity.member.identities.filter((i) => i.verified)
      } else {
        metadata.verifiedIdentities = payload.activity.objectMember.identities.filter(
          (i) => i.verified,
        )
      }

      if (membersWithIdentity.size > 0) {
        metadata.memberWithIdentity = membersWithIdentity.values().next().value
      }

      if (dbMember) {
        metadata.memberIdToUpdate = dbMember.id
        metadata.memberType = memberType

        if (memberType === 'member') {
          metadata.memberSource = payload.dbMemberSource
        } else {
          metadata.memberSource = payload.dbObjectMemberSource
        }
      }

      if (
        metadata.memberWithIdentity &&
        metadata.memberIdToUpdate &&
        metadata.memberWithIdentity !== metadata.memberIdToUpdate
      ) {
        // lets just merge the members
        const originalId = metadata.memberWithIdentity as string
        const targetId = metadata.memberIdToUpdate as string

        // but first check memberNoMerge table
        const noMergeMemberIds = await getMemberNoMerge(this.pgQx, [originalId, targetId])

        const noMerge = singleOrDefault(
          noMergeMemberIds,
          (m) =>
            (m.memberId === originalId && m.noMergeId === targetId) ||
            (m.memberId === targetId && m.noMergeId === originalId),
        )

        if (noMerge) {
          metadata.noMerge = true
        } else {
          try {
            await this.pgQx.tx(async (txPgQx) => {
              const service = new CommonMemberService(txPgQx, this.temporal, this.log)
              await service.merge(originalId, targetId)
            })

            return originalId
          } catch (err) {
            metadata.mergeError = {
              errorMessage: err?.message ?? '<no error message>',
              errorStack: err?.stack,
              err,
            }
          }
        }
      }

      return metadata
    }

    if (error instanceof ApplicationError) {
      let nextError: any = error.originalError

      while (nextError) {
        if (checkForIdentityConstraint(nextError)) {
          return extractMetadata(nextError)
        } else if (nextError instanceof ApplicationError) {
          nextError = nextError.originalError
        } else {
          nextError = undefined
        }
      }
    } else if (checkForIdentityConstraint(error)) {
      return extractMetadata(error)
    }

    return undefined
  }

  private needsActivityRelationUpdate(
    existing: IDbActivityRelation,
    newData: IActivityRelationCreateOrUpdateData,
  ): boolean {
    // Compare all fields that would be updated in the ON CONFLICT clause
    return (
      existing.memberId !== newData.memberId ||
      existing.objectMemberId !== (newData.objectMemberId ?? null) ||
      existing.organizationId !== (newData.organizationId ?? null) ||
      existing.platform !== newData.platform ||
      existing.username !== newData.username ||
      existing.objectMemberUsername !== (newData.objectMemberUsername ?? null) ||
      existing.sourceId !== newData.sourceId ||
      existing.sourceParentId !== (newData.sourceParentId ?? null) ||
      existing.type !== newData.type ||
      existing.timestamp !== newData.timestamp ||
      existing.channel !== newData.channel ||
      existing.sentimentScore !== newData.sentimentScore ||
      existing.gitInsertions !== newData.gitInsertions ||
      existing.gitDeletions !== newData.gitDeletions ||
      existing.score !== newData.score ||
      existing.pullRequestReviewState !== (newData.pullRequestReviewState ?? null)
    )
  }

  private areTheSameMember(
    platform: PlatformType,
    member1: IMemberData,
    member2: IMemberData,
  ): boolean {
    const m1Identities = (member1.identities || []).filter(
      (i) => i.verified && i.platform === platform,
    )
    const m2Identities = (member2.identities || []).filter(
      (i) => i.verified && i.platform === platform,
    )

    for (const i1 of m1Identities) {
      for (const i2 of m2Identities) {
        if (i1.type === i2.type && i1.value === i2.value) {
          return true
        }
      }
    }

    return false
  }

  private memberAttValue(
    attName: MemberAttributeName,
    member: IMemberData,
    platform: PlatformType,
    dbMember?: IDbMember,
  ): unknown {
    let result: unknown
    if (dbMember && dbMember.attributes[attName]) {
      // db member already has this attribute
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const att = dbMember.attributes[attName] as any
      // if it's manually set we use that
      if (att.custom) {
        // manually set
        result = att.custom
      } else if (att.system) {
        // system set (e.g., BotDetectionService or LLM service)
        result = att.system
      } else {
        // if it's not manually set we check if incoming member data has the attribute set for the platform
        if (member.attributes[attName] && member.attributes[attName][platform]) {
          result = member.attributes[attName][platform]
        } else {
          // if none of those work we just use db member attribute default value
          result = att.default
        }
      }
    } else if (member.attributes[attName] && member.attributes[attName][platform]) {
      result = member.attributes[attName][platform]
    }

    return result
  }

  public async getActivitySentiment(
    activity: ISentimentActivityInput,
  ): Promise<ISentimentAnalysisResult | undefined> {
    const settings = DEFAULT_ACTIVITY_TYPE_SETTINGS[activity.platform]?.[activity.type]

    if (settings && settings.calculateSentiment === true) {
      const ALLOWED_MAX_BYTE_LENGTH = 5000

      const prepareText = (text: string): string => {
        // https://docs.aws.amazon.com/comprehend/latest/APIReference/API_DetectSentiment.html
        // needs to be utf-8 encoded
        let prepared = Buffer.from(text).toString('utf8')
        // from docs - AWS performs analysis on the first 500 characters and ignores the rest

        // Remove Non-ASCII characters
        // eslint-disable-next-line no-control-regex
        prepared = prepared.replace(/[^\x00-\x7F]/g, '')

        prepared = prepared.slice(0, 500)
        // trim down to max allowed byte length
        prepared = trimUtf8ToMaxByteLength(prepared, ALLOWED_MAX_BYTE_LENGTH)

        return prepared.trim()
      }

      const text = `${activity.body || ''} ${activity.title || ''}`.trim()

      const preparedText = prepareText(text)

      if (preparedText.length === 0) {
        return undefined
      }

      const sentiment = vader.SentimentIntensityAnalyzer.polarity_scores(preparedText)
      const compound = Math.round(((sentiment.compound + 1) / 2) * 100)
      // Some activities are inherently different, we might want to dampen their sentiment

      let label = 'neutral'
      if (compound < 33) {
        label = 'negative'
      } else if (compound > 66) {
        label = 'positive'
      }

      return {
        positive: Math.round(sentiment.pos * 100),
        negative: Math.round(sentiment.neg * 100),
        neutral: Math.round(sentiment.neu * 100),
        mixed: Math.round(sentiment.neu * 100),
        sentiment: compound,
        label,
      }
    }

    return null
  }
}

interface IActivityProcessData {
  resultId: string
  integrationId: string
  onboarding: boolean
  platform: PlatformType
  activity: IActivityData
  segmentId: string

  dbMember?: IDbMember
  dbObjectMember?: IDbMember
  dbActivityRelation?: IDbActivityRelation
  dbMemberSource?: 'activity' | 'username' | 'email'
  dbObjectMemberSource?: 'activity' | 'username' | 'email'

  organizationId?: string
  memberId?: string
  objectMemberId?: string
  activityId?: string
}

interface IActivityPrepareForUpsertResult {
  resultId: string
  activityId: string
  typeToCreate?: string
  channelToCreate?: string

  payload?: IDbActivityCreateData
}
