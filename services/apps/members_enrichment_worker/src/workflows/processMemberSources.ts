import { proxyActivities } from '@temporalio/workflow'

import { MemberEnrichmentSource, PlatformType } from '@crowd/types'

import * as activities from '../activities'
import { IMemberEnrichmentDataNormalized, IProcessMemberSourcesArgs } from '../types'

const {
  findMemberEnrichmentCache,
  normalizeEnrichmentData,
  fetchMemberDataForLLMSquashing,
  findWhichLinkedinProfileToUseAmongScraperResult,
  squashMultipleValueAttributesWithLLM,
  squashWorkExperiencesWithLLM,
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

export async function processMemberSources(args: IProcessMemberSourcesArgs): Promise<boolean> {
  // without contributions since they take a lot of space
  const toBeSquashed = {}

  let hasContributions = false

  // find if there's already saved enrichment data in source
  const caches = await findMemberEnrichmentCache(args.sources, args.memberId)
  for (const source of args.sources) {
    const cache = caches.find((c) => c.source === source)
    if (cache && cache.data) {
      const normalized = (await normalizeEnrichmentData(
        source,
        cache.data,
      )) as IMemberEnrichmentDataNormalized

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
      }

      if (normalized.contributions) {
        delete normalized.contributions
      }

      if (normalized.reach) {
        delete normalized.reach
      }

      toBeSquashed[source] = normalized
    }
  }

  if (Object.keys(toBeSquashed).length > 1) {
    const existingMemberData = await fetchMemberDataForLLMSquashing(args.memberId)

    let progaiLinkedinScraperProfileSelected: IMemberEnrichmentDataNormalized = null
    let crustDataProfileSelected: IMemberEnrichmentDataNormalized = null

    if (toBeSquashed[MemberEnrichmentSource.CRUSTDATA]) {
      const categorizationResult = await findWhichLinkedinProfileToUseAmongScraperResult(
        args.memberId,
        existingMemberData,
        toBeSquashed[MemberEnrichmentSource.CRUSTDATA],
      )

      crustDataProfileSelected = categorizationResult.selected

      if (crustDataProfileSelected) {
        toBeSquashed[MemberEnrichmentSource.CRUSTDATA] = crustDataProfileSelected
      }

      // check if there are any discarded profiles
      if (categorizationResult.discarded.length > 0) {
        for (const discardedProfile of categorizationResult.discarded) {
          const discardedLinkedinIdentity = discardedProfile.identities.find(
            (i) => i.platform === PlatformType.LINKEDIN,
          )

          // Skip if no LinkedIn identity found
          if (!discardedLinkedinIdentity) {
            continue
          }

          // remove the root source where the discarded linkedin profile is coming from
          for (const source of Object.keys(toBeSquashed)) {
            if (
              (toBeSquashed[source].identities || []).some(
                (i) =>
                  i.value === discardedLinkedinIdentity.value &&
                  i.platform === PlatformType.LINKEDIN,
              )
            ) {
              delete toBeSquashed[source]
            }
          }
        }
      }
    }

    if (toBeSquashed[MemberEnrichmentSource.PROGAI_LINKEDIN_SCRAPER]) {
      const categorizationResult = await findWhichLinkedinProfileToUseAmongScraperResult(
        args.memberId,
        existingMemberData,
        toBeSquashed[MemberEnrichmentSource.PROGAI_LINKEDIN_SCRAPER],
      )

      progaiLinkedinScraperProfileSelected = categorizationResult.selected

      if (progaiLinkedinScraperProfileSelected) {
        toBeSquashed[MemberEnrichmentSource.PROGAI_LINKEDIN_SCRAPER] =
          progaiLinkedinScraperProfileSelected
      }

      if (categorizationResult.discarded.length > 0) {
        for (const discardedProfile of categorizationResult.discarded) {
          const discardedLinkedinIdentity = discardedProfile.identities.find(
            (i) => i.platform === PlatformType.LINKEDIN,
          )

          // Skip if no LinkedIn identity found
          if (!discardedLinkedinIdentity) {
            continue
          }

          // remove the root source where the discarded linkedin profile is coming from
          for (const source of Object.keys(toBeSquashed)) {
            if (
              (toBeSquashed[source].identities || []).some(
                (i) =>
                  i.value === discardedLinkedinIdentity.value &&
                  i.platform === PlatformType.LINKEDIN,
              )
            ) {
              delete toBeSquashed[source]
            }
          }
        }
      }
    }

    // start squashing the data
    const squashedPayload: IMemberEnrichmentDataNormalized = {
      identities: [],
      attributes: {},
      memberOrganizations: [],
      reach: {},
    }

    // 1) squash identities
    for (const source of Object.keys(toBeSquashed)) {
      if (toBeSquashed[source].identities) {
        for (const identity of toBeSquashed[source].identities) {
          // check if identity already exists, if not add it to squashedPayload
          if (
            !squashedPayload.identities.find(
              (i) =>
                i.platform === identity.platform &&
                i.type === identity.type &&
                i.value === identity.value,
            ) &&
            // check in member data as well
            !existingMemberData.identities.find(
              (i) =>
                i.platform === identity.platform &&
                i.type === identity.type &&
                i.value === identity.value,
            )
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
      if (toBeSquashed[source].attributes) {
        for (const attribute of Object.keys(toBeSquashed[source].attributes)) {
          if (toBeSquashed[source].attributes[attribute][`enrichment-${source}`]) {
            if (attributeCountMap[attribute]) {
              attributeCountMap[attribute] = attributeCountMap[attribute] + 1
              delete attributesSquashed[attribute]
              attributeValues[attribute].push(
                toBeSquashed[source].attributes[attribute][`enrichment-${source}`],
              )
            } else {
              attributeCountMap[attribute] = 1
              attributesSquashed[attribute] = {
                enrichment: toBeSquashed[source].attributes[attribute][`enrichment-${source}`],
              }
              attributeValues[attribute] = [
                toBeSquashed[source].attributes[attribute][`enrichment-${source}`],
              ]
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
        if (
          toBeSquashed[source].memberOrganizations &&
          toBeSquashed[source].memberOrganizations.length > 0
        ) {
          workExperienceDataInDifferentSources.push(toBeSquashed[source].memberOrganizations)
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
      progaiLinkedinScraperProfileSelected && hasContributions,
      !!crustDataProfileSelected,
    )

    return memberUpdated
  }

  await touchMemberEnrichmentLastTriedAt(args.memberId)

  return false
}
