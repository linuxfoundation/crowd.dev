import { get as getLevenshteinDistance } from 'fast-levenshtein'

import { getDomainRootLabel } from '@crowd/common'
import {
  IOrganizationFullAggregatesOpensearch,
  IOrganizationIdentity,
  OrganizationIdentityType,
} from '@crowd/types'

class OrganizationSimilarityCalculator {
  static HIGH_CONFIDENCE_SCORE = 0.9
  static MEDIUM_CONFIDENCE_SCORE = 0.75
  static LOW_CONFIDENCE_SCORE = 0.2

  static calculateSimilarity(
    primaryOrganization: IOrganizationFullAggregatesOpensearch,
    similarOrganization: IOrganizationFullAggregatesOpensearch,
  ): number {
    let smallestEditDistance: number = null

    let similarPrimaryIdentity: IOrganizationIdentity = null

    const sameDisplayName =
      Boolean(primaryOrganization.displayName) &&
      Boolean(similarOrganization.displayName) &&
      primaryOrganization.displayName.toLowerCase() ===
        similarOrganization.displayName.toLowerCase()

    // Organizations can have multiple usernames on one platform; don't let that clash
    // veto a shared verified primary-domain and same displayName.
    if (
      sameDisplayName &&
      this.hasSharedVerifiedPrimaryDomain(primaryOrganization, similarOrganization)
    ) {
      return 0.85
    }

    if (this.hasClashingOrganizationIdentities(primaryOrganization, similarOrganization)) {
      return this.LOW_CONFIDENCE_SCORE
    }

    const verifiedIdentities = primaryOrganization.identities.filter((i) => i.verified)

    // find the smallest edit distance between both identity arrays
    for (const primaryIdentity of verifiedIdentities) {
      // similar organization has an unverified identity as one of primary organization's strong identity, return score 95
      if (
        similarOrganization.identities.some(
          (wi) =>
            !wi.verified &&
            wi.value === primaryIdentity.value &&
            wi.type === primaryIdentity.type &&
            wi.platform === primaryIdentity.platform,
        )
      ) {
        return 0.95
      }

      // check displayName match
      if (sameDisplayName) {
        return this.decideSimilarityUsingAdditionalChecks(primaryOrganization, similarOrganization)
      }

      // check for domain similarity (e.g., amazon.com vs amazon.at)
      if (this.hasSimilarDomainPattern(primaryIdentity, similarOrganization.identities)) {
        return this.decideSimilarityUsingAdditionalChecks(
          primaryOrganization,
          similarOrganization,
          0.85,
        )
      }

      for (const secondaryIdentity of similarOrganization.identities) {
        const currentLevenstheinDistance = getLevenshteinDistance(
          primaryIdentity.value,
          secondaryIdentity.value,
        )
        if (smallestEditDistance === null || smallestEditDistance > currentLevenstheinDistance) {
          smallestEditDistance = currentLevenstheinDistance
          similarPrimaryIdentity = primaryIdentity
        }
      }
    }

    // calculate similarity percentage
    const identityLength = similarPrimaryIdentity?.value.length || 0

    if (identityLength < smallestEditDistance) {
      return this.LOW_CONFIDENCE_SCORE
    }

    return this.decideSimilarityUsingAdditionalChecks(
      primaryOrganization,
      similarOrganization,
      Math.floor(((identityLength - smallestEditDistance) / identityLength) * 100) / 100,
    )
  }

  static decideSimilarityUsingAdditionalChecks(
    organization: IOrganizationFullAggregatesOpensearch,
    similarOrganization: IOrganizationFullAggregatesOpensearch,
    startingScore?: number,
  ): number {
    let isHighConfidence = false
    let confidenceScore = startingScore || this.HIGH_CONFIDENCE_SCORE

    const bumpFactor = Math.floor((1 - confidenceScore) / 5)

    if (this.hasSameLocation(organization, similarOrganization)) {
      isHighConfidence = true
      confidenceScore = this.bumpConfidenceScore(confidenceScore, bumpFactor)
    }

    if (this.hasSameIndustry(organization, similarOrganization)) {
      isHighConfidence = true
      confidenceScore = this.bumpConfidenceScore(confidenceScore, bumpFactor)
    }

    if (this.hasSameTicker(organization, similarOrganization)) {
      isHighConfidence = true
      confidenceScore = this.bumpConfidenceScore(confidenceScore, bumpFactor)
    }

    if (!isHighConfidence) {
      return this.MEDIUM_CONFIDENCE_SCORE
    }

    return confidenceScore
  }

  static bumpConfidenceScore(confidenceScore: number, bump: number): number {
    return Math.min(1, confidenceScore + bump)
  }

  static hasSharedVerifiedPrimaryDomain(
    organization: IOrganizationFullAggregatesOpensearch,
    similarOrganization: IOrganizationFullAggregatesOpensearch,
  ): boolean {
    const primaryDomains = organization.identities
      .filter((i) => i.verified && i.type === OrganizationIdentityType.PRIMARY_DOMAIN)
      .map((i) => i.value.toLowerCase())

    if (primaryDomains.length === 0) {
      return false
    }

    return similarOrganization.identities.some(
      (i) =>
        i.verified &&
        i.type === OrganizationIdentityType.PRIMARY_DOMAIN &&
        primaryDomains.includes(i.value.toLowerCase()),
    )
  }

  static hasClashingOrganizationIdentities(
    organization: IOrganizationFullAggregatesOpensearch,
    similarOrganization: IOrganizationFullAggregatesOpensearch,
  ): boolean {
    const verifiedIdentities = organization.identities.filter((i) => i.verified)

    for (const identity of verifiedIdentities) {
      if (
        similarOrganization.identities.some(
          (i) =>
            i.verified &&
            i.platform === identity.platform &&
            i.type === identity.type &&
            i.value !== identity.value &&
            this.isConflictingIdentity(identity, i),
        )
      ) {
        return true
      }
    }

    return false
  }

  static isConflictingIdentity(
    identity1: IOrganizationIdentity,
    identity2: IOrganizationIdentity,
  ): boolean {
    // Both identities must have the same type and platform to be compared
    if (identity1.type !== identity2.type || identity1.platform !== identity2.platform) {
      return false
    }

    // For domain-based identities, different domains are not necessarily conflicting
    // They could be regional domains, subsidiaries, etc.
    if (
      identity1.type === OrganizationIdentityType.PRIMARY_DOMAIN ||
      identity1.type === OrganizationIdentityType.ALTERNATIVE_DOMAIN
    ) {
      return false
    }

    // For username-based identities, different usernames are conflicting
    // as they represent different accounts
    if (identity1.type === OrganizationIdentityType.USERNAME) {
      return true
    }

    // For other types, treat as conflicting if different values
    return true
  }

  static hasSameLocation(
    organization: IOrganizationFullAggregatesOpensearch,
    similarOrganization: IOrganizationFullAggregatesOpensearch,
  ): boolean {
    return (
      organization.location &&
      similarOrganization.location &&
      organization.location.toLowerCase() === similarOrganization.location.toLowerCase()
    )
  }

  static hasSameWebsite(
    organization: IOrganizationFullAggregatesOpensearch,
    similarOrganization: IOrganizationFullAggregatesOpensearch,
  ): boolean {
    return (
      organization.website &&
      similarOrganization.website &&
      organization.website.toLowerCase() === similarOrganization.website.toLowerCase()
    )
  }

  static hasSameIndustry(
    organization: IOrganizationFullAggregatesOpensearch,
    similarOrganization: IOrganizationFullAggregatesOpensearch,
  ): boolean {
    return (
      organization.industry &&
      similarOrganization.industry &&
      organization.industry.toLowerCase() === similarOrganization.industry.toLowerCase()
    )
  }

  static hasSameTicker(
    organization: IOrganizationFullAggregatesOpensearch,
    similarOrganization: IOrganizationFullAggregatesOpensearch,
  ): boolean {
    return (
      organization.ticker &&
      similarOrganization.ticker &&
      organization.ticker.toLowerCase() === similarOrganization.ticker.toLowerCase()
    )
  }

  static hasSimilarDomainPattern(
    primaryIdentity: IOrganizationIdentity,
    similarIdentities: IOrganizationIdentity[],
  ): boolean {
    const primaryDomain = primaryIdentity.value.toLowerCase()

    for (const similarIdentity of similarIdentities) {
      // Only consider verified identities for domain similarity to avoid false positives
      if (!similarIdentity.verified) {
        continue
      }

      if (
        similarIdentity.type !== OrganizationIdentityType.PRIMARY_DOMAIN &&
        similarIdentity.type !== OrganizationIdentityType.ALTERNATIVE_DOMAIN
      ) {
        continue
      }

      const similarDomain = similarIdentity.value.toLowerCase()

      if (this.hasSameDomainRootLabel(primaryDomain, similarDomain)) {
        return true
      }
    }

    return false
  }

  static hasSameDomainRootLabel(domain1: string, domain2: string): boolean {
    const main1 = getDomainRootLabel(domain1)
    const main2 = getDomainRootLabel(domain2)

    return main1 !== null && main2 !== null && main1 === main2
  }
}

export default OrganizationSimilarityCalculator
