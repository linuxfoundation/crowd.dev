import { proxyActivities } from '@temporalio/workflow'

import { MemberEnrichmentSource, PlatformType } from '@crowd/types'

import * as activities from '../activities'
import { IMemberEnrichmentDataNormalized, IProcessMemberSourcesArgs } from '../types'
import { SINGLE_SOURCE_ENRICHMENT_ATTRIBUTES } from '../utils/config'

const {
  findMemberEnrichmentCache,
  normalizeEnrichmentData,
  fetchMemberDataForLLMSquashing,
  findWhichLinkedinProfileToUseAmongScraperResult,
  updateMemberUsingSquashedPayload,
  cleanAttributeValue,
  touchMemberEnrichmentLastTriedAt,
} = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
  retry: {
    initialInterval: '15s',
    backoffCoefficient: 2.0,
    maximumInterval: '60s',
    maximumAttempts: 4,
  },
})

const { squashMultipleValueAttributesWithLLM, squashWorkExperiencesWithLLM } = proxyActivities<
  typeof activities
>({
  startToCloseTimeout: '10 minutes',
  retry: {
    initialInterval: '30s',
    backoffCoefficient: 2.0,
    maximumInterval: '5 minutes',
    maximumAttempts: 6,
  },
})

function getEnrichmentAttributeValue(
  attributes: IMemberEnrichmentDataNormalized['attributes'],
  attributeName: string,
) {
  const values = attributes?.[attributeName]
  if (!values) {
    return undefined
  }

  const enrichmentKey = Object.keys(values).find((key) => key.startsWith('enrichment-'))
  return enrichmentKey ? values[enrichmentKey] : undefined
}

export async function processMemberSources(args: IProcessMemberSourcesArgs): Promise<boolean> {
  // without contributions since they take a lot of space
  const toBeSquashed: Record<
    string,
    IMemberEnrichmentDataNormalized | IMemberEnrichmentDataNormalized[]
  > = {}

  let hasContributions = false

  // find if there's already saved enrichment data in source
  const caches = await findMemberEnrichmentCache(args.sources, args.memberId)
  for (const source of args.sources) {
    const cache = caches.find((c) => c.source === source)
    if (cache && cache.data) {
      const normalized = (await normalizeEnrichmentData(source, cache.data)) as
        | IMemberEnrichmentDataNormalized
        | IMemberEnrichmentDataNormalized[]

      if (Array.isArray(normalized)) {
        for (const n of normalized) {
          if (n.contributions) {
            if (n.contributions.length > 0) {
              hasContributions = true
            }
            delete n.contributions
          }

          if (n.reach) {
            delete n.reach
          }
        }
      } else {
        if (normalized.contributions) {
          delete normalized.contributions
        }

        if (normalized.reach) {
          delete normalized.reach
        }
      }

      toBeSquashed[source] = normalized
    }
  }

  let existingMemberData = null

  const arraySources = Object.keys(toBeSquashed).filter((source) =>
    Array.isArray(toBeSquashed[source]),
  )

  // Only resolve arrays when an apply path can run afterward.
  if (
    arraySources.length > 0 &&
    (Object.keys(toBeSquashed).length === 1 || args.activityCount > 100)
  ) {
    existingMemberData = await fetchMemberDataForLLMSquashing(args.memberId)

    const orderedArraySources = [
      ...arraySources.filter((source) => source === MemberEnrichmentSource.CRUSTDATA),
      ...arraySources.filter((source) => source === MemberEnrichmentSource.PROGAI_LINKEDIN_SCRAPER),
      ...arraySources.filter(
        (source) =>
          source !== MemberEnrichmentSource.CRUSTDATA &&
          source !== MemberEnrichmentSource.PROGAI_LINKEDIN_SCRAPER,
      ),
    ]

    for (const source of orderedArraySources) {
      const normalized = toBeSquashed[source]
      if (!Array.isArray(normalized)) {
        continue
      }

      const categorizationResult = await findWhichLinkedinProfileToUseAmongScraperResult(
        args.memberId,
        existingMemberData,
        normalized,
      )

      if (categorizationResult.selected) {
        toBeSquashed[source] = categorizationResult.selected
      } else {
        delete toBeSquashed[source]
      }

      // check if there are any discarded profiles
      if (categorizationResult.discarded.length > 0) {
        for (const discardedProfile of categorizationResult.discarded) {
          const discardedLinkedinIdentity = discardedProfile.identities?.find(
            (i) => i.platform === PlatformType.LINKEDIN,
          )

          // Skip if no LinkedIn identity found
          if (!discardedLinkedinIdentity) {
            continue
          }

          // remove the root source where the discarded linkedin profile is coming from
          for (const otherSource of Object.keys(toBeSquashed)) {
            const profile = toBeSquashed[otherSource]
            if (Array.isArray(profile)) {
              continue
            }

            if (
              (profile.identities || []).some(
                (i) =>
                  i.value.trim().toLowerCase() ===
                    discardedLinkedinIdentity.value.trim().toLowerCase() &&
                  i.platform === PlatformType.LINKEDIN,
              )
            ) {
              delete toBeSquashed[otherSource]
            }
          }
        }
      }
    }
  }

  const sourceKeys = Object.keys(toBeSquashed)

  // A single source isn't reliable enough for a full profile update, but some attributes
  // are still useful for aggregate consumers, where coverage matters more than certainty.
  if (sourceKeys.length === 1) {
    const source = sourceKeys[0]
    const normalized = toBeSquashed[source] as IMemberEnrichmentDataNormalized

    if (normalized.attributes) {
      const attributes = {}

      for (const attributeName of SINGLE_SOURCE_ENRICHMENT_ATTRIBUTES) {
        const value = getEnrichmentAttributeValue(normalized.attributes, attributeName)
        if (value) {
          attributes[attributeName] = {
            enrichment: await cleanAttributeValue(value),
          }
        }
      }

      if (Object.keys(attributes).length > 0) {
        if (!existingMemberData) {
          existingMemberData = await fetchMemberDataForLLMSquashing(args.memberId)
        }

        return updateMemberUsingSquashedPayload(
          args.memberId,
          existingMemberData,
          {
            identities: [],
            attributes,
            memberOrganizations: [],
            reach: {},
          },
          false,
          false,
        )
      }
    }
  }

  if (sourceKeys.length > 1 && args.activityCount > 100) {
    if (!existingMemberData) {
      existingMemberData = await fetchMemberDataForLLMSquashing(args.memberId)
    }

    const crustDataProfileSelected = toBeSquashed[
      MemberEnrichmentSource.CRUSTDATA
    ] as IMemberEnrichmentDataNormalized
    const progaiLinkedinScraperProfileSelected = toBeSquashed[
      MemberEnrichmentSource.PROGAI_LINKEDIN_SCRAPER
    ] as IMemberEnrichmentDataNormalized

    // start squashing the data
    const squashedPayload: IMemberEnrichmentDataNormalized = {
      identities: [],
      attributes: {},
      memberOrganizations: [],
      reach: {},
    }

    // 1) squash identities
    for (const source of Object.keys(toBeSquashed)) {
      const profile = toBeSquashed[source] as IMemberEnrichmentDataNormalized
      if (profile.identities) {
        for (const identity of profile.identities) {
          const sameIdentity = (i: { platform: string; type: string; value: string }) =>
            i.platform === identity.platform &&
            i.type === identity.type &&
            i.value.trim().toLowerCase() === identity.value.trim().toLowerCase()

          if (
            !squashedPayload.identities.find(sameIdentity) &&
            !existingMemberData.identities.find(sameIdentity)
          ) {
            squashedPayload.identities.push(identity)
          }
        }
      }
    }

    const attributesSquashed = {}
    const attributeCountMap = {}
    const attributeValues = {}

    // 2) squash attributes
    for (const source of Object.keys(toBeSquashed)) {
      const profile = toBeSquashed[source] as IMemberEnrichmentDataNormalized
      if (profile.attributes) {
        for (const attribute of Object.keys(profile.attributes)) {
          const value = getEnrichmentAttributeValue(profile.attributes, attribute)
          if (value) {
            if (attributeCountMap[attribute]) {
              attributeCountMap[attribute] = attributeCountMap[attribute] + 1
              delete attributesSquashed[attribute]
              attributeValues[attribute].push(value)
            } else {
              attributeCountMap[attribute] = 1
              attributesSquashed[attribute] = {
                enrichment: value,
              }
              attributeValues[attribute] = [value]
            }
          }
        }
      }
    }

    const llmInputAttributes = {}

    for (const attribute of Object.keys(attributeCountMap)) {
      if (attributeCountMap[attribute] === 1) {
        attributesSquashed[attribute] = {
          enrichment: await cleanAttributeValue(attributeValues[attribute][0]),
        }
      } else {
        llmInputAttributes[attribute] = await Promise.all(
          attributeValues[attribute].map((v) => cleanAttributeValue(v)),
        )
      }
    }

    if (Object.keys(llmInputAttributes).length > 0) {
      // ask LLM to select from multiple values in different sources for the same attribute
      const multipleValueAttributesSquashed = await squashMultipleValueAttributesWithLLM(
        args.memberId,
        llmInputAttributes,
      )

      for (const attribute of Object.keys(multipleValueAttributesSquashed)) {
        if (multipleValueAttributesSquashed[attribute]) {
          attributesSquashed[attribute] = {
            enrichment: await cleanAttributeValue(multipleValueAttributesSquashed[attribute]),
          }
        }
      }
    }

    squashedPayload.attributes = attributesSquashed

    // 3) squash work experiences
    if (crustDataProfileSelected) {
      squashedPayload.memberOrganizations = crustDataProfileSelected.memberOrganizations
    } else {
      // check if there are multiple work experiences from different sources
      const workExperienceDataInDifferentSources = []
      for (const source of Object.keys(toBeSquashed)) {
        const profile = toBeSquashed[source] as IMemberEnrichmentDataNormalized
        if (profile.memberOrganizations && profile.memberOrganizations.length > 0) {
          workExperienceDataInDifferentSources.push(profile.memberOrganizations)
        }
      }

      if (workExperienceDataInDifferentSources.length == 0) {
        squashedPayload.memberOrganizations = []
      } else if (workExperienceDataInDifferentSources.length == 1) {
        squashedPayload.memberOrganizations = workExperienceDataInDifferentSources[0]
      } else {
        const workExperiencesSquashedByLLM = await squashWorkExperiencesWithLLM(
          args.memberId,
          workExperienceDataInDifferentSources,
        )
        // if there are multiple verified identities in work experiences, we reduce it
        // to one because in our db they might exist in different organizations and
        // might need a merge. To avoid this, we'll only send the org with one verified identity
        workExperiencesSquashedByLLM.forEach((we) => {
          let found = false
          we.identities = (we.identities || []).map((i) => {
            if (i.verified && !found) {
              found = true
              return i
            } else if (i.verified) {
              return { ...i, verified: false }
            }
            return i
          })
        })
        squashedPayload.memberOrganizations = workExperiencesSquashedByLLM
      }
    }

    // 4) handle reach - it can only come from crustdata
    if (crustDataProfileSelected && crustDataProfileSelected.reach) {
      squashedPayload.reach = crustDataProfileSelected.reach
    }

    const memberUpdated = await updateMemberUsingSquashedPayload(
      args.memberId,
      existingMemberData,
      squashedPayload,
      !!progaiLinkedinScraperProfileSelected && hasContributions,
      !!crustDataProfileSelected,
    )

    return memberUpdated
  }

  await touchMemberEnrichmentLastTriedAt(args.memberId)

  return false
}
