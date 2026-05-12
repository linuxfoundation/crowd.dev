/* eslint-disable @typescript-eslint/no-explicit-any */
import uniqBy from 'lodash.uniqby'

import { OrganizationField, findOrgById, queryOrgs } from '@crowd/data-access-layer'
import { hasLfxMembership } from '@crowd/data-access-layer/src/lfx_memberships'
import OrganizationMergeSuggestionsRepository from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/organizationMergeSuggestions.repo'
import { addOrgNoMerge } from '@crowd/data-access-layer/src/org_merge'
import { fetchOrgIdentities, findOrgAttributes } from '@crowd/data-access-layer/src/organizations'
import { QueryExecutor, pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { buildFullOrgForMergeSuggestions } from '@crowd/opensearch'
import {
  ILLMConsumableOrganization,
  IOrganizationBaseForMergeSuggestions,
  IOrganizationFullAggregatesOpensearch,
  IOrganizationIdentity,
  IOrganizationMergeSuggestion,
  IOrganizationOpensearch,
  OpenSearchIndex,
  OrganizationIdentityType,
  OrganizationMergeSuggestionTable,
} from '@crowd/types'

import { svc } from '../main'
import OrganizationSimilarityCalculator from '../organizationSimilarityCalculator'
import {
  ISimilarOrganizationOpensearchResult,
  ISimilarityFilter,
  OpenSearchQueryClauseBuilder,
} from '../types'
import { chunkArray, isNumeric, prefixLength } from '../utils'

export async function getOrganizations(
  tenantId: string,
  batchSize: number,
  afterOrganizationId?: string,
  lastGeneratedAt?: string,
  organizationIds?: string[],
): Promise<IOrganizationBaseForMergeSuggestions[]> {
  try {
    const qx = pgpQx(svc.postgres.reader.connection())
    const rows = await queryOrgs(qx, {
      filter: {
        and: [
          { [OrganizationField.TENANT_ID]: { eq: tenantId } },
          afterOrganizationId ? { [OrganizationField.ID]: { gt: afterOrganizationId } } : null,
          // Include organizations updated after the last generation to cover both new and modified ones
          lastGeneratedAt ? { [OrganizationField.UPDATED_AT]: { gt: lastGeneratedAt } } : null,
          organizationIds ? { [OrganizationField.ID]: { in: organizationIds } } : null,
          // TODO filter by organizationIds,
        ],
      },
      fields: [
        OrganizationField.ID,
        OrganizationField.TENANT_ID,
        OrganizationField.DISPLAY_NAME,
        OrganizationField.LOCATION,
        OrganizationField.INDUSTRY,
      ],
      orderBy: `${OrganizationField.ID} asc`,
      limit: batchSize,
    })

    return rows
  } catch (err) {
    throw new Error(err)
  }
}

export async function findTenantsLatestOrganizationSuggestionGeneratedAt(
  tenantId: string,
): Promise<string> {
  const organizationMergeSuggestionsRepo = new OrganizationMergeSuggestionsRepository(
    svc.postgres.writer.connection(),
    svc.log,
  )
  return organizationMergeSuggestionsRepo.findTenantsLatestOrganizationSuggestionGeneratedAt(
    tenantId,
  )
}

export async function updateOrganizationMergeSuggestionsLastGeneratedAt(
  tenantId: string,
): Promise<void> {
  const organizationMergeSuggestionsRepo = new OrganizationMergeSuggestionsRepository(
    svc.postgres.writer.connection(),
    svc.log,
  )
  await organizationMergeSuggestionsRepo.updateOrganizationMergeSuggestionsLastGeneratedAt(tenantId)
}

export async function getOrganizationMergeSuggestions(
  tenantId: string,
  organization: IOrganizationBaseForMergeSuggestions,
): Promise<IOrganizationMergeSuggestion[]> {
  // Helper to convert OpenSearch organization response to organization object
  function opensearchToFullOrg(
    organization: IOrganizationOpensearch,
  ): IOrganizationFullAggregatesOpensearch {
    return {
      id: organization.uuid_organizationId,
      displayName: organization.keyword_displayName,
      location: organization.string_location,
      industry: organization.string_industry,
      ticker: organization.string_ticker,
      identities: organization.nested_identities.map((identity) => ({
        platform: identity.string_platform,
        type: identity.string_type as OrganizationIdentityType,
        value: identity.string_value,
        verified: identity.bool_verified,
        source: identity.string_source,
      })),
      website: organization.string_website,
      activityCount: organization.int_activityCount,
      noMergeIds: [],
    }
  }

  const mergeSuggestions: IOrganizationMergeSuggestion[] = []
  const organizationMergeSuggestionsRepo = new OrganizationMergeSuggestionsRepository(
    svc.postgres.writer.connection(),
    svc.log,
  )

  // Build full organization with all identities and attributes
  const qx = pgpQx(svc.postgres.reader.connection())
  const fullOrg = await buildFullOrgForMergeSuggestions(qx, organization)

  // Early return if no identities to match against
  if (fullOrg.identities.length === 0) {
    return []
  }

  // Get organizations that should not be merged
  const noMergeIds = await organizationMergeSuggestionsRepo.findNoMergeIds(fullOrg.id)
  const excludeIds = [fullOrg.id]

  if (noMergeIds && noMergeIds.length > 0) {
    excludeIds.push(...noMergeIds)
  }

  // Deduplicate and sort verified identities first
  const identities = uniqBy(fullOrg.identities, (i) => `${i.platform}:${i.value}`).sort((a, b) =>
    a.verified === b.verified ? 0 : a.verified ? -1 : 1,
  )

  // Categorize identities into different query types
  const weakIdentities = []
  const fuzzyIdentities = []
  const prefixIdentities = []

  const cleanIdentityValue = (value: string, platform: string): string => {
    let cleaned = value.replace(/^https?:\/\//, '')

    if (platform === 'linkedin') {
      cleaned = cleaned.split(':').pop() || cleaned
    }

    return cleaned.toLowerCase()
  }

  // Process up to 75 identities
  // This is a safety limit to prevent OpenSearch max clause errors
  for (let i = 0; i < Math.min(identities.length, 75); i++) {
    const { value: rawValue, platform } = identities[i]
    if (!rawValue) continue // skip invalid

    const value = cleanIdentityValue(rawValue, platform)

    // All identities go into weak query (exact match)
    weakIdentities.push({ value: rawValue, platform })

    // Skip numeric-only values
    if (isNumeric(value)) continue

    // Fuzzy match candidates
    fuzzyIdentities.push({ value, platform })

    // Prefix match candidates: long enough + no spaces
    const hasNoSpaces = !value.includes(' ')
    if (value.length > 5 && hasNoSpaces) {
      prefixIdentities.push({
        value: value.slice(0, prefixLength(value)),
        platform,
      })
    }
  }

  // Build OpenSearch query clauses
  const identitiesShould = []
  const CHUNK_SIZE = 15 // Split queries into chunks to avoid OpenSearch limits

  const clauseBuilders: OpenSearchQueryClauseBuilder<Partial<IOrganizationIdentity>>[] = [
    {
      // Query 1: Weak identities - exact match on unverified identities
      matches: weakIdentities,
      builder: ({ value, platform }) => ({
        bool: {
          must: [
            { match: { [`nested_identities.string_value`]: value } },
            { match: { [`nested_identities.string_platform`]: platform } },
            { term: { [`nested_identities.bool_verified`]: false } },
          ],
        },
      }),
    },
    {
      // Query 2: Fuzzy matching - find similar values with typos/variations (verified only)
      matches: uniqBy(fuzzyIdentities, 'value'),
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
    {
      // Query 3: Prefix matching - find values that start with our prefix (verified only)
      matches: uniqBy(prefixIdentities, 'value'),
      builder: ({ value }) => ({
        prefix: {
          [`nested_identities.string_value`]: {
            value,
            rewrite: 'top_terms_3',
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
          minimum_should_match: 1,
        },
      },
    },
  }

  // Main query: match by display name OR any of the identity queries
  const identitiesPartialQuery = {
    should: [
      {
        term: {
          [`keyword_displayName`]: fullOrg.displayName,
        },
      },
      ...(identitiesShould.length > 0 ? [nestedIdentityQuery] : []),
    ],
    minimum_should_match: 1,
    must_not: [
      {
        terms: {
          uuid_organizationId: excludeIds,
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

  // Build final OpenSearch query with fields to return
  const similarOrganizationsQueryBody = {
    query: {
      bool: identitiesPartialQuery,
    },
    _source: [
      'uuid_organizationId',
      'uuid_tenantId',
      'nested_identities',
      'nested_weakIdentities',
      'keyword_displayName',
      'string_location',
      'string_industry',
      'string_website',
      'string_ticker',
      'int_activityCount',
    ],
  }

  // Check if primary org has LFX membership (used to filter suggestions)
  const primaryOrgWithLfxMembership = await hasLfxMembership(qx, {
    organizationId: fullOrg.id,
  })

  let organizationsToMerge: ISimilarOrganizationOpensearchResult[]

  // Execute OpenSearch query to find similar organizations
  try {
    organizationsToMerge =
      (
        await svc.opensearch.client.search({
          index: OpenSearchIndex.ORGANIZATIONS,
          body: similarOrganizationsQueryBody,
        })
      ).body?.hits?.hits || []
  } catch (e) {
    svc.log.error(
      { error: e, query: identitiesPartialQuery },
      'Error while searching for similar organizations!',
    )
    throw e
  }

  // Process each candidate organization and calculate similarity
  for (const organizationToMerge of organizationsToMerge) {
    const secondaryOrgWithLfxMembership = await hasLfxMembership(qx, {
      organizationId: organizationToMerge._source.uuid_organizationId,
    })

    // Skip if both organizations have LFX membership (don't merge LFX organizations)
    if (primaryOrgWithLfxMembership && secondaryOrgWithLfxMembership) {
      continue
    }

    // Calculate similarity score between organizations
    const similarityConfidenceScore = OrganizationSimilarityCalculator.calculateSimilarity(
      fullOrg,
      opensearchToFullOrg(organizationToMerge._source),
    )

    // Sort organizations: primary has more identities/activity, secondary is the one to merge
    const organizationsSorted = [fullOrg, opensearchToFullOrg(organizationToMerge._source)].sort(
      (a, b) => {
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
      },
    )

    mergeSuggestions.push({
      similarity: similarityConfidenceScore,
      organizations: [organizationsSorted[0].id, organizationsSorted[1].id],
    })
  }

  return mergeSuggestions
}

export async function addOrganizationToMerge(
  suggestions: IOrganizationMergeSuggestion[],
  table: OrganizationMergeSuggestionTable,
): Promise<void> {
  if (suggestions.length > 0) {
    const organizationMergeSuggestionsRepo = new OrganizationMergeSuggestionsRepository(
      svc.postgres.writer.connection(),
      svc.log,
    )
    await organizationMergeSuggestionsRepo.addToMerge(suggestions, table)
  }
}

async function prepareOrg(
  qx: QueryExecutor,
  organizationId: string,
): Promise<ILLMConsumableOrganization> {
  const [base, identities, attributes] = await Promise.all([
    findOrgById(qx, organizationId, [
      OrganizationField.ID,
      OrganizationField.DISPLAY_NAME,
      OrganizationField.DESCRIPTION,
      OrganizationField.LOGO,
      OrganizationField.TAGS,
      OrganizationField.LOCATION,
      OrganizationField.TYPE,
      OrganizationField.HEADLINE,
      OrganizationField.INDUSTRY,
      OrganizationField.FOUNDED,
    ]),
    fetchOrgIdentities(qx, organizationId),
    findOrgAttributes(qx, organizationId),
  ])

  if (!base) {
    return null
  }

  return {
    displayName: base?.displayName || '',
    description: base.description,
    phoneNumbers: attributes.filter((a) => a.name === 'phoneNumber').map((a) => a.value),
    logo: base.logo,
    tags: base.tags,
    location: base.location,
    type: base.type,
    geoLocation: attributes.find((a) => a.name === 'geoLocation')?.value || '',
    ticker: attributes.find((a) => a.name === 'ticker')?.value || '',
    profiles: attributes.filter((a) => a.name === 'profile').map((a) => a.value),
    headline: base.headline,
    industry: base.industry,
    founded: base.founded,
    alternativeNames: attributes.filter((a) => a.name === 'alternativeName').map((a) => a.value),
    identities: identities.map((i) => ({
      platform: i.platform,
      value: i.value,
    })),
  }
}

export async function getOrganizationsForLLMConsumption(
  organizationIds: string[],
): Promise<ILLMConsumableOrganization[]> {
  const qx = pgpQx(svc.postgres.reader.connection())
  const result = await Promise.all(
    organizationIds.map((orgId) => {
      return prepareOrg(qx, orgId)
    }),
  )

  return result.filter((r) => r !== null)
}

export async function getRawOrganizationMergeSuggestions(
  tenantId: string,
  similarityFilter: ISimilarityFilter,
  limit: number,
  onlyLFXMembers = false,
  organizationIds: string[] = [],
): Promise<string[][]> {
  const organizationMergeSuggestionsRepo = new OrganizationMergeSuggestionsRepository(
    svc.postgres.writer.connection(),
    svc.log,
  )

  const suggestions = await organizationMergeSuggestionsRepo.getRawOrganizationSuggestions(
    similarityFilter,
    limit,
    onlyLFXMembers,
    organizationIds,
  )
  if (onlyLFXMembers) {
    // make sure primary is lfx member
    for (let i = 0; i < suggestions.length; i++) {
      const qx = pgpQx(svc.postgres.reader.connection())
      const isPrimaryOrgInSuggestionLFXMember = await hasLfxMembership(qx, {
        organizationId: suggestions[i][0],
      })

      if (!isPrimaryOrgInSuggestionLFXMember) {
        suggestions[i] = [suggestions[i][1], suggestions[i][0]]
      }
    }
  }

  return suggestions
}

export async function removeOrganizationMergeSuggestions(
  suggestion: string[],
  table: OrganizationMergeSuggestionTable,
): Promise<void> {
  const organizationMergeSuggestionsRepo = new OrganizationMergeSuggestionsRepository(
    svc.postgres.writer.connection(),
    svc.log,
  )
  await organizationMergeSuggestionsRepo.removeOrganizationMergeSuggestions(suggestion, table)
}

export async function addOrganizationSuggestionToNoMerge(suggestion: string[]): Promise<void> {
  if (suggestion.length !== 2) {
    svc.log.debug('Suggestions array must have exactly two ids!')
    return
  }

  const qx = pgpQx(svc.postgres.writer.connection())

  try {
    await addOrgNoMerge(qx, suggestion[0], suggestion[1])
  } catch (error: unknown) {
    // Handle foreign key constraint violation gracefully
    if (error instanceof Error && 'code' in error && error.code === '23503') {
      svc.log.info({ suggestion }, 'Foreign key constraint violation, skipping no merge!')
      return
    }

    svc.log.error({ error, suggestion }, 'Error adding organization suggestion to no merge!')
    throw error
  }
}
