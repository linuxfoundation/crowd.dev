/* eslint-disable @typescript-eslint/no-explicit-any */
import uniqBy from 'lodash.uniqby'

import { parseGitHubNoreplyEmail } from '@crowd/common'
import { addMemberNoMerge } from '@crowd/data-access-layer/src/member_merge'
import { MemberField, queryMembers } from '@crowd/data-access-layer/src/members'
import MemberMergeSuggestionsRepository from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/memberMergeSuggestions.repo'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { buildFullMemberForMergeSuggestions } from '@crowd/opensearch'
import {
  ILLMConsumableMember,
  IMemberBaseForMergeSuggestions,
  IMemberIdentity,
  IMemberMergeSuggestion,
  MemberIdentityType,
  MemberMergeSuggestionTable,
  OpenSearchIndex,
  PlatformType,
} from '@crowd/types'

import { EMAIL_AS_USERNAME_PLATFORMS } from '../enums'
import { svc } from '../main'
import MemberSimilarityCalculator from '../memberSimilarityCalculator'
import {
  ISimilarMemberOpensearchResult,
  ISimilarityFilter,
  OpenSearchQueryClauseBuilder,
} from '../types'
import { chunkArray, isEmailAsUsernamePlatform, isNumeric, stripProtocol } from '../utils'

/**
 * Finds similar members of given member in a tenant
 * Members are similar if:
 * - they have exactly same or similar looking identities (with max levenshtein distance = 2)
 * - they have exactly same emails
 * - they have exactly same display name
 * @param tenantId
 * @param member
 * @returns similar members in an array with calculated similarity score and activityEstimate
 * Activity estimate is calculated by adding activity counts of both members
 */
export async function getMemberMergeSuggestions(
  tenantId: string,
  member: IMemberBaseForMergeSuggestions,
): Promise<IMemberMergeSuggestion[]> {
  const mergeSuggestions: IMemberMergeSuggestion[] = []
  const memberMergeSuggestionsRepo = new MemberMergeSuggestionsRepository(
    svc.postgres.writer.connection(),
    svc.log,
  )

  const qx = pgpQx(svc.postgres.reader.connection())
  const fullMember = await buildFullMemberForMergeSuggestions(qx, member)

  // Deduplicate and sort verified identities first
  const identities = uniqBy(fullMember.identities, (i) => `${i.platform}:${i.value}`).sort(
    (a, b) => (a.verified === b.verified ? 0 : a.verified ? -1 : 1),
  )

  // Early return if no identities to match against
  if (identities.length === 0) {
    return []
  }

  // Get members that should not be merged
  const noMergeIds = await memberMergeSuggestionsRepo.findNoMergeIds(member.id)
  const excludeIds = [fullMember.id]
  if (noMergeIds && noMergeIds.length > 0) {
    excludeIds.push(...noMergeIds)
  }

  // Categorize identities into different query types
  const verifiedExactMatches = []
  const verifiedEmailUsernameMatches = []
  const verifiedUsernameEmailMatches = []
  const verifiedFuzzyMatches = []

  const unverifiedExactMatches = []
  const unverifiedEmailUsernameMatches = []
  const unverifiedUsernameEmailMatches = []

  // Noreply email -> platform username matches
  // e.g. "123+john@users.noreply.github.com" -> GitHub username "john"
  const noreplyEmailUsernameMatches = []

  // Process up to 75 identities
  // This is a safety limit to prevent OpenSearch max clause errors
  for (const { verified, value, platform, type } of identities.slice(0, 75)) {
    const isEmail = type === MemberIdentityType.EMAIL
    const isUsername = type === MemberIdentityType.USERNAME
    const isEmailAsUsername = isUsername && isEmailAsUsernamePlatform(platform)

    const targetLists = verified
      ? {
          exact: verifiedExactMatches,
          emailUsername: verifiedEmailUsernameMatches,
          usernameEmail: verifiedUsernameEmailMatches,
          fuzzy: verifiedFuzzyMatches,
        }
      : {
          exact: unverifiedExactMatches,
          emailUsername: unverifiedEmailUsernameMatches,
          usernameEmail: unverifiedUsernameEmailMatches,
        }

    // Exact matches
    targetLists.exact.push({ value, platform })

    // Email-as-username cases
    if (isEmail) {
      targetLists.emailUsername.push({ value })
    } else if (isEmailAsUsername) {
      targetLists.usernameEmail.push({ value })
    }

    // Noreply email -> platform username extraction
    if (isEmail && verified) {
      const ghUsername = parseGitHubNoreplyEmail(value)
      if (ghUsername) {
        noreplyEmailUsernameMatches.push({ value: ghUsername, platform: PlatformType.GITHUB })
      }
    }

    // Fuzzy matches (only for verified & non-numeric)
    if (verified && !isNumeric(value)) {
      targetLists.fuzzy.push({ value: stripProtocol(value) })
    }
  }

  // Build OpenSearch query clauses
  const identitiesShould = []
  const CHUNK_SIZE = 15 // Split queries into chunks to avoid OpenSearch limits

  const clauseBuilders: OpenSearchQueryClauseBuilder<Partial<IMemberIdentity>>[] = [
    {
      // Query 1: Verified -> Unverified exact matches
      matches: verifiedExactMatches,
      builder: ({ value, platform }) => ({
        bool: {
          must: [
            { term: { [`nested_identities.keyword_value`]: value } },
            { match: { [`nested_identities.string_platform`]: platform } },
            { term: { [`nested_identities.bool_verified`]: false } },
          ],
        },
      }),
    },
    {
      // Query 2: Verified email -> Unverified username (email-as-username platforms)
      matches: verifiedEmailUsernameMatches,
      builder: ({ value }) => ({
        bool: {
          must: [
            { term: { [`nested_identities.keyword_value`]: value } },
            { terms: { [`nested_identities.string_platform`]: EMAIL_AS_USERNAME_PLATFORMS } },
            { term: { [`nested_identities.keyword_type`]: MemberIdentityType.USERNAME } },
            { term: { [`nested_identities.bool_verified`]: false } },
          ],
        },
      }),
    },
    {
      // Query 3: Verified username -> Unverified email (email-as-username platforms)
      matches: verifiedUsernameEmailMatches,
      builder: ({ value }) => ({
        bool: {
          must: [
            { term: { [`nested_identities.keyword_value`]: value } },
            { term: { [`nested_identities.keyword_type`]: MemberIdentityType.EMAIL } },
            { term: { [`nested_identities.bool_verified`]: false } },
          ],
        },
      }),
    },
    {
      // Query 5: Unverified -> Verified exact matches
      matches: unverifiedExactMatches,
      builder: ({ value, platform }) => ({
        bool: {
          must: [
            { term: { [`nested_identities.keyword_value`]: value } },
            { match: { [`nested_identities.string_platform`]: platform } },
            { term: { [`nested_identities.bool_verified`]: true } },
          ],
        },
      }),
    },
    {
      // Query 6: Unverified email -> Verified username (email-as-username platforms)
      matches: unverifiedEmailUsernameMatches,
      builder: ({ value }) => ({
        bool: {
          must: [
            { term: { [`nested_identities.keyword_value`]: value } },
            { terms: { [`nested_identities.string_platform`]: EMAIL_AS_USERNAME_PLATFORMS } },
            { term: { [`nested_identities.keyword_type`]: MemberIdentityType.USERNAME } },
            { term: { [`nested_identities.bool_verified`]: true } },
          ],
        },
      }),
    },
    {
      // Query 7: Unverified username -> Verified email (email-as-username platforms)
      matches: unverifiedUsernameEmailMatches,
      builder: ({ value }) => ({
        bool: {
          must: [
            { term: { [`nested_identities.keyword_value`]: value } },
            { term: { [`nested_identities.keyword_type`]: MemberIdentityType.EMAIL } },
            { term: { [`nested_identities.bool_verified`]: true } },
          ],
        },
      }),
    },
    {
      // Query 8: Noreply/private email -> username (verified or unverified)
      matches: uniqBy(noreplyEmailUsernameMatches, 'value'),
      builder: ({ value, platform }) => ({
        bool: {
          must: [
            { term: { [`nested_identities.keyword_value`]: value } },
            { match: { [`nested_identities.string_platform`]: platform } },
            { term: { [`nested_identities.keyword_type`]: MemberIdentityType.USERNAME } },
          ],
        },
      }),
    },
    {
      // Query 9: Verified -> Verified fuzzy matches
      matches: uniqBy(verifiedFuzzyMatches, 'value'),
      builder: ({ value }) => ({
        match: {
          [`nested_identities.string_value`]: {
            query: value,
            prefix_length: 1,
            fuzziness: 'auto',
            max_expansions: 3,
          },
        },
      }),
      filter: [{ term: { [`nested_identities.bool_verified`]: true } }],
    },
  ]

  for (const clauseBuilder of clauseBuilders) {
    const { matches, builder, filter } = clauseBuilder
    if (matches.length > 0) {
      const chunks = chunkArray(matches, CHUNK_SIZE)
      for (const chunk of chunks) {
        const shouldClauses = chunk.map(builder)
        const chunkQuery: any = {
          bool: {
            should: shouldClauses,
            minimum_should_match: 1,
          },
        }
        if (filter) {
          chunkQuery.bool.filter = filter
        }
        identitiesShould.push(chunkQuery)
      }
    }
  }

  // Wrap all identity queries in a nested query (identities are nested documents)
  const nestedIdentityQuery = {
    nested: {
      path: 'nested_identities',
      query: {
        bool: {
          should: identitiesShould,
          boost: 1,
          minimum_should_match: 1,
        },
      },
    },
  }

  // Main query: match by display name OR any of the identity queries
  const identitiesPartialQuery: any = {
    should: [
      {
        term: {
          [`keyword_displayName`]: fullMember.displayName,
        },
      },
      ...(identitiesShould.length > 0 ? [nestedIdentityQuery] : []),
    ],
    minimum_should_match: 1,
    must_not: [
      {
        terms: {
          uuid_memberId: excludeIds,
        },
      },
    ],
    filter: [
      {
        term: {
          uuid_tenantId: tenantId,
        },
      },
    ],
  }

  const similarMembersQueryBody = {
    query: {
      bool: identitiesPartialQuery,
    },
    _source: [
      'uuid_memberId',
      'keyword_displayName',
      'int_activityCount',
      'nested_identities',
      'obj_attributes',
      'nested_organizations',
    ],
  }

  let membersToMerge: ISimilarMemberOpensearchResult[]

  try {
    membersToMerge =
      (
        await svc.opensearch.client.search({
          index: OpenSearchIndex.MEMBERS,
          body: similarMembersQueryBody,
        })
      ).body?.hits?.hits || []
  } catch (e) {
    svc.log.error(
      { error: e, query: identitiesPartialQuery },
      'Error while searching for similar members!',
    )
    throw e
  }

  for (const memberToMerge of membersToMerge) {
    const similarityConfidenceScore = MemberSimilarityCalculator.calculateSimilarity(
      fullMember,
      memberToMerge._source,
    )
    // decide the primary member using number of activities & number of identities
    const membersSorted = [
      fullMember,
      {
        id: memberToMerge._source.uuid_memberId,
        activityCount: memberToMerge._source.int_activityCount,
        identities: memberToMerge._source.nested_identities,
      },
    ].sort((a, b) => {
      if (
        a.identities.length > b.identities.length ||
        (a.identities.length === b.identities.length && a.activityCount > b.activityCount)
      ) {
        return -1
      } else if (
        a.identities.length < b.identities.length ||
        (a.identities.length === b.identities.length && a.activityCount < b.activityCount)
      ) {
        return 1
      }
      return 0
    })
    mergeSuggestions.push({
      similarity: similarityConfidenceScore,
      activityEstimate:
        (membersSorted[0].activityCount || 0) + (membersSorted[1].activityCount || 0),
      members: [membersSorted[0].id, membersSorted[1].id],
    })
  }

  return mergeSuggestions
}

export async function addMemberToMerge(
  suggestions: IMemberMergeSuggestion[],
  table: MemberMergeSuggestionTable,
): Promise<void> {
  if (suggestions.length > 0) {
    const memberMergeSuggestionsRepo = new MemberMergeSuggestionsRepository(
      svc.postgres.writer.connection(),
      svc.log,
    )
    await memberMergeSuggestionsRepo.addToMerge(suggestions, table)
  }
}

export async function findTenantsLatestMemberSuggestionGeneratedAt(
  tenantId: string,
): Promise<string> {
  const memberMergeSuggestionsRepo = new MemberMergeSuggestionsRepository(
    svc.postgres.writer.connection(),
    svc.log,
  )
  return memberMergeSuggestionsRepo.findTenantsLatestMemberSuggestionGeneratedAt(tenantId)
}

export async function updateMemberMergeSuggestionsLastGeneratedAt(tenantId: string): Promise<void> {
  const memberMergeSuggestionsRepo = new MemberMergeSuggestionsRepository(
    svc.postgres.writer.connection(),
    svc.log,
  )
  await memberMergeSuggestionsRepo.updateMemberMergeSuggestionsLastGeneratedAt(tenantId)
}

export async function getMembers(
  tenantId: string,
  batchSize: number,
  afterMemberId?: string,
  lastGeneratedAt?: string,
): Promise<IMemberBaseForMergeSuggestions[]> {
  try {
    const qx = pgpQx(svc.postgres.reader.connection())
    const rows = await queryMembers(qx, {
      filter: {
        and: [
          { [MemberField.TENANT_ID]: { eq: tenantId } },
          afterMemberId ? { [MemberField.ID]: { gt: afterMemberId } } : null,
          // Include members updated after the last generation to cover both new and modified ones
          lastGeneratedAt ? { [MemberField.UPDATED_AT]: { gt: lastGeneratedAt } } : null,
        ],
      },
      fields: [
        MemberField.ID,
        MemberField.TENANT_ID,
        MemberField.DISPLAY_NAME,
        MemberField.ATTRIBUTES,
      ],
      orderBy: `${MemberField.ID} asc`,
      limit: batchSize,
    })

    return rows
  } catch (err) {
    throw new Error(err)
  }
}

export async function getMembersForLLMConsumption(
  memberIds: string[],
): Promise<ILLMConsumableMember[]> {
  const memberMergeSuggestionsRepo = new MemberMergeSuggestionsRepository(
    svc.postgres.writer.connection(),
    svc.log,
  )
  const [primaryMember, secondaryMember] = await memberMergeSuggestionsRepo.getMembers(memberIds)

  const result: ILLMConsumableMember[] = []

  if (primaryMember) {
    result.push({
      displayName: primaryMember.displayName,
      joinedAt: primaryMember.joinedAt,
      attributes: primaryMember.attributes,
      identities: primaryMember.identities.map((i) => ({ platform: i.platform, value: i.value })),
      organizations: primaryMember.organizations.map((o) => ({
        title: o.title,
        logo: o.logo,
        displayName: o.displayName,
        dateEnd: o.dateEnd,
        dateStart: o.dateStart,
      })),
    })
  }

  if (secondaryMember) {
    result.push({
      joinedAt: secondaryMember.joinedAt,
      displayName: secondaryMember.displayName,
      attributes: secondaryMember.attributes,
      identities: secondaryMember.identities.map((i) => ({ platform: i.platform, value: i.value })),
      organizations: secondaryMember.organizations.map((o) => ({
        title: o.title,
        logo: o.logo,
        displayName: o.displayName,
        dateEnd: o.dateEnd,
        dateStart: o.dateStart,
      })),
    })
  }

  return result
}

export async function getRawMemberMergeSuggestions(
  similarityFilter: ISimilarityFilter,
  limit: number,
): Promise<string[][]> {
  const memberMergeSuggestionsRepo = new MemberMergeSuggestionsRepository(
    svc.postgres.writer.connection(),
    svc.log,
  )
  return memberMergeSuggestionsRepo.getRawMemberSuggestions(similarityFilter, limit)
}

export async function removeMemberMergeSuggestion(
  suggestion: string[],
  table: MemberMergeSuggestionTable,
): Promise<void> {
  const memberMergeSuggestionsRepo = new MemberMergeSuggestionsRepository(
    svc.postgres.writer.connection(),
    svc.log,
  )
  await memberMergeSuggestionsRepo.removeMemberMergeSuggestion(suggestion, table)
}

export async function addMemberSuggestionToNoMerge(suggestion: string[]): Promise<void> {
  if (suggestion.length !== 2) {
    svc.log.debug(`Suggestions array must have two ids!`)
    return
  }
  const qx = pgpQx(svc.postgres.writer.connection())

  await addMemberNoMerge(qx, suggestion[0], suggestion[1])
}
