import { get as getLevenshteinDistance } from 'fast-levenshtein'

import { parseGitHubNoreplyEmail } from '@crowd/common'
import {
  IMemberIdentity,
  IMemberOpensearch,
  IMemberOrganization,
  IMemberOrganizationOpensearch,
  IMemberWithAggregatesForMergeSuggestions,
  MemberAttributeName,
  MemberIdentityType,
  PlatformType,
} from '@crowd/types'

import { EMAIL_AS_USERNAME_PLATFORMS } from './enums'
import { MemberAttributeOpensearch } from './enums'

class MemberSimilarityCalculator {
  static HIGH_CONFIDENCE_SCORE = 0.9
  static LOW_CONFIDENCE_SCORE = 0.2

  static calculateSimilarity(
    primaryMember: IMemberWithAggregatesForMergeSuggestions,
    similarMember: IMemberOpensearch,
  ): number {
    let smallestEditDistance: number = null

    let similarPrimaryIdentity: IMemberIdentity = null

    const primaryMemberVerifiedEmails = primaryMember.identities
      .filter((i) => i.verified && i.type === MemberIdentityType.EMAIL)
      .map((i) => i.value)

    const primaryMemberUnverifiedEmails = primaryMember.identities
      .filter((i) => !i.verified && i.type === MemberIdentityType.EMAIL)
      .map((i) => i.value)

    const similarMemberVerifiedEmails = similarMember.nested_identities
      .filter((i) => i.bool_verified && i.keyword_type === MemberIdentityType.EMAIL)
      .map((i) => i.string_value)

    const similarMemberUnverifiedEmails = similarMember.nested_identities
      .filter((i) => !i.bool_verified && i.keyword_type === MemberIdentityType.EMAIL)
      .map((i) => i.string_value)

    // return a small confidence score when there's clashing identities
    // clashing identities are username identities in same platform and different values
    if (this.hasClashingMemberIdentities(primaryMember, similarMember)) {
      return 0.2
    }

    // We check if there are any verified<->unverified email matches between primary & similar members
    if (
      (similarMemberUnverifiedEmails &&
        similarMemberUnverifiedEmails.length > 0 &&
        similarMemberUnverifiedEmails.some((email) =>
          primaryMemberVerifiedEmails.includes(email),
        )) ||
      (similarMemberVerifiedEmails &&
        similarMemberVerifiedEmails.length > 0 &&
        similarMemberVerifiedEmails.some((email) => primaryMemberUnverifiedEmails.includes(email)))
    ) {
      return 0.98
    }

    // check primary unverified identity <-> secondary verified identity exact match
    for (const primaryIdentity of primaryMember.identities.filter((i) => !i.verified)) {
      if (
        similarMember.nested_identities &&
        similarMember.nested_identities.length > 0 &&
        similarMember.nested_identities.some(
          (verifiedIdentity) =>
            verifiedIdentity.bool_verified &&
            verifiedIdentity.string_value === primaryIdentity.value &&
            verifiedIdentity.keyword_type === primaryIdentity.type &&
            verifiedIdentity.string_platform === primaryIdentity.platform,
        )
      ) {
        return 0.98
      }

      // Unverified email matches verified username on a platform that uses emails as usernames
      if (
        primaryIdentity.type === MemberIdentityType.EMAIL &&
        this.hasEmailAsUsernameIdentityMatch(
          primaryIdentity,
          similarMember,
          MemberIdentityType.USERNAME,
          true,
        )
      ) {
        return 0.95
      }

      // Unverified username matches verified email on a platform that uses emails as usernames
      if (
        primaryIdentity.type === MemberIdentityType.USERNAME &&
        this.hasEmailAsUsernameIdentityMatch(
          primaryIdentity,
          similarMember,
          MemberIdentityType.EMAIL,
          true,
        )
      ) {
        return 0.95
      }
    }

    // Check if a noreply email on one member resolves to a username on the other
    if (this.hasMatchingUsernameFromNoreplyEmail(primaryMember, similarMember)) {
      return 0.95
    }

    for (const primaryIdentity of primaryMember.identities.filter((i) => i.verified)) {
      // similar member has an unverified identity as one of primary members's verified identity, return score 95
      if (
        similarMember.nested_identities &&
        similarMember.nested_identities.length > 0 &&
        similarMember.nested_identities.some(
          (unverifiedIdentity) =>
            unverifiedIdentity.bool_verified === false &&
            unverifiedIdentity.string_value === primaryIdentity.value &&
            unverifiedIdentity.keyword_type === primaryIdentity.type &&
            unverifiedIdentity.string_platform === primaryIdentity.platform,
        )
      ) {
        return 0.95
      }

      // Verified email matches unverified username on a platform that uses emails as usernames
      if (
        primaryIdentity.type === MemberIdentityType.EMAIL &&
        this.hasEmailAsUsernameIdentityMatch(
          primaryIdentity,
          similarMember,
          MemberIdentityType.USERNAME,
          false,
        )
      ) {
        return 0.95
      }

      // Verified username matches unverified email on a platform that uses emails as usernames
      if (
        primaryIdentity.type === MemberIdentityType.USERNAME &&
        this.hasEmailAsUsernameIdentityMatch(
          primaryIdentity,
          similarMember,
          MemberIdentityType.EMAIL,
          false,
        )
      ) {
        return 0.95
      }

      for (const secondaryIdentity of similarMember.nested_identities.filter(
        (i) => i.bool_verified,
      )) {
        const currentLevenstheinDistance = getLevenshteinDistance(
          `${primaryIdentity.value}`,
          `${secondaryIdentity.string_value}`,
        )
        if (smallestEditDistance === null || smallestEditDistance > currentLevenstheinDistance) {
          smallestEditDistance = currentLevenstheinDistance
          similarPrimaryIdentity = primaryIdentity
        }
      }
    }

    // check displayName match
    if (
      similarMember.keyword_displayName.toLowerCase() === primaryMember.displayName.toLowerCase()
    ) {
      return this.decideMemberSimilarityUsingAdditionalChecks(primaryMember, similarMember)
    }

    // calculate similarity percentage
    const identityLength = similarPrimaryIdentity?.value.length || 0

    if (identityLength < smallestEditDistance) {
      return this.LOW_CONFIDENCE_SCORE
    }

    return this.decideMemberSimilarityUsingAdditionalChecks(
      primaryMember,
      similarMember,
      Math.floor(((identityLength - smallestEditDistance) / identityLength) * 100) / 100,
    )
  }

  static hasClashingMemberIdentities(
    member: IMemberWithAggregatesForMergeSuggestions,
    similarMember: IMemberOpensearch,
  ): boolean {
    if (member.identities && member.identities.length > 0) {
      for (const identity of member.identities.filter(
        (i) => i.type === MemberIdentityType.USERNAME,
      )) {
        if (
          similarMember.nested_identities.some(
            (i) =>
              i.keyword_type === MemberIdentityType.USERNAME &&
              i.string_platform === identity.platform &&
              i.keyword_value !== identity.value,
          )
        ) {
          return true
        }
      }
    }

    return false
  }

  /**
   * Checks if a noreply email in one member matches a username in the other (e.g. GitHub noreply email -> GitHub username)
   * Works bidirectionally: primary email -> similar username, and similar email -> primary username.
   */
  static hasMatchingUsernameFromNoreplyEmail(
    primaryMember: IMemberWithAggregatesForMergeSuggestions,
    similarMember: IMemberOpensearch,
  ): boolean {
    // Primary member's noreply emails -> similar member's platform usernames
    const similarUsernamesByPlatform = {
      [PlatformType.GITHUB]: new Set(
        similarMember.nested_identities
          .filter(
            (i) =>
              i.string_platform === PlatformType.GITHUB &&
              i.keyword_type === MemberIdentityType.USERNAME,
          )
          .map((i) => i.string_value?.toLowerCase()),
      ),
    }

    for (const identity of primaryMember.identities) {
      if (!identity.verified || identity.type !== MemberIdentityType.EMAIL) continue

      const ghUsername = parseGitHubNoreplyEmail(identity.value)
      if (ghUsername && similarUsernamesByPlatform[PlatformType.GITHUB].has(ghUsername)) {
        return true
      }
    }

    // Similar member's noreply emails -> primary member's platform usernames
    const primaryUsernamesByPlatform = {
      [PlatformType.GITHUB]: new Set(
        primaryMember.identities
          .filter(
            (i) => i.platform === PlatformType.GITHUB && i.type === MemberIdentityType.USERNAME,
          )
          .map((i) => i.value?.toLowerCase()),
      ),
    }

    for (const identity of similarMember.nested_identities) {
      if (!identity.bool_verified || identity.keyword_type !== MemberIdentityType.EMAIL) continue

      const ghUsername = parseGitHubNoreplyEmail(identity.string_value)
      if (ghUsername && primaryUsernamesByPlatform[PlatformType.GITHUB].has(ghUsername)) {
        return true
      }
    }

    return false
  }

  static hasEmailAsUsernameIdentityMatch(
    primaryIdentity: IMemberIdentity,
    similarMember: IMemberOpensearch,
    targetType: MemberIdentityType,
    isVerified: boolean,
  ): boolean {
    return (
      similarMember.nested_identities &&
      similarMember.nested_identities.length > 0 &&
      similarMember.nested_identities.some(
        (identity) =>
          identity.bool_verified === isVerified &&
          identity.string_value === primaryIdentity.value &&
          identity.keyword_type === targetType &&
          EMAIL_AS_USERNAME_PLATFORMS.includes(identity.string_platform as PlatformType),
      )
    )
  }

  static hasSameLocation(
    member: IMemberWithAggregatesForMergeSuggestions,
    similarMember: IMemberOpensearch,
  ): boolean {
    const primaryMemberLocation = this.getLocation(member)
    const similarMemberLocation = this.getLocation(similarMember)

    return (
      primaryMemberLocation &&
      similarMemberLocation &&
      primaryMemberLocation.toLowerCase() === similarMemberLocation.toLowerCase()
    )
  }

  static hasSameAvatarUrl(
    member: IMemberWithAggregatesForMergeSuggestions,
    similarMember: IMemberOpensearch,
  ): boolean {
    const primaryMemberAvatar = this.getAvatarUrl(member)
    const similarMemberAvatar = this.getAvatarUrl(similarMember)

    return (
      primaryMemberAvatar &&
      similarMemberAvatar &&
      primaryMemberAvatar.toLowerCase() === similarMemberAvatar.toLowerCase()
    )
  }

  static hasSameTimezone(
    member: IMemberWithAggregatesForMergeSuggestions,
    similarMember: IMemberOpensearch,
  ): boolean {
    const primaryMemberTimezone = this.getTimezone(member)
    const similarMemberTimezone = this.getTimezone(similarMember)

    return (
      primaryMemberTimezone &&
      similarMemberTimezone &&
      primaryMemberTimezone.toLowerCase() === similarMemberTimezone.toLowerCase()
    )
  }

  static hasIntersectingLanguages(
    member: IMemberWithAggregatesForMergeSuggestions,
    similarMember: IMemberOpensearch,
  ): boolean {
    const primaryMemberLanguages = this.getLanguages(member).filter((l) => l !== 'English')
    const similarMemberLanguages = this.getLanguages(similarMember).filter((l) => l !== 'English')

    return (
      primaryMemberLanguages &&
      primaryMemberLanguages.length > 0 &&
      similarMemberLanguages &&
      similarMemberLanguages.length > 0 &&
      primaryMemberLanguages.some((l) => similarMemberLanguages.includes(l))
    )
  }

  static hasIntersectingProgrammingLanguages(
    member: IMemberWithAggregatesForMergeSuggestions,
    similarMember: IMemberOpensearch,
  ): boolean {
    const primaryMemberProgrammingLanguages = this.getProgrammingLanguages(member)
    const similarMemberProgrammingLanguages = this.getProgrammingLanguages(similarMember)

    return (
      primaryMemberProgrammingLanguages &&
      primaryMemberProgrammingLanguages.length > 0 &&
      similarMemberProgrammingLanguages &&
      similarMemberProgrammingLanguages.length > 0 &&
      primaryMemberProgrammingLanguages.some((l) => similarMemberProgrammingLanguages.includes(l))
    )
  }

  static hasRolesInSameOrganization(
    member: IMemberWithAggregatesForMergeSuggestions,
    similarMember: IMemberOpensearch,
  ): boolean {
    for (const memberRoles of member.organizations) {
      if (
        similarMember.nested_organizations.some(
          (o) => memberRoles.displayName === o.string_displayName,
        )
      ) {
        return true
      }
    }
    return false
  }

  static decideMemberSimilarityUsingAdditionalChecks(
    member: IMemberWithAggregatesForMergeSuggestions,
    similarMember: IMemberOpensearch,
    startingScore?: number,
  ): number {
    let isHighConfidence = false
    let confidenceScore = startingScore || this.HIGH_CONFIDENCE_SCORE

    const bumpFactor = Math.floor((1 - confidenceScore) / 5)

    if (this.hasSameLocation(member, similarMember)) {
      isHighConfidence = true
      confidenceScore = this.bumpConfidenceScore(confidenceScore, bumpFactor)
    }

    if (this.hasRolesInSameOrganization(member, similarMember)) {
      isHighConfidence = true
      confidenceScore = this.bumpConfidenceScore(confidenceScore, bumpFactor)
    }

    if (this.hasIntersectingLanguages(member, similarMember)) {
      isHighConfidence = true
      confidenceScore = this.bumpConfidenceScore(confidenceScore, bumpFactor)
    }

    if (this.hasIntersectingProgrammingLanguages(member, similarMember)) {
      isHighConfidence = true
      confidenceScore = this.bumpConfidenceScore(confidenceScore, bumpFactor)
    }

    if (this.hasSameTimezone(member, similarMember)) {
      isHighConfidence = true
      confidenceScore = this.bumpConfidenceScore(confidenceScore, bumpFactor)
    }

    if (!isHighConfidence) {
      return this.LOW_CONFIDENCE_SCORE
    }

    return confidenceScore
  }

  static bumpConfidenceScore(confidenceScore: number, bump: number): number {
    return Math.min(1, confidenceScore + bump)
  }

  static getLocation(member: IMemberOpensearch | IMemberWithAggregatesForMergeSuggestions): string {
    if (this.isSimilarMemberFromOpensearch(member)) {
      return member.obj_attributes[MemberAttributeOpensearch.LOCATION]?.string_default || null
    } else if (this.isFullMemberFromDb(member)) {
      return member.attributes[MemberAttributeName.LOCATION]?.default || null
    }
    return null
  }

  static getAvatarUrl(
    member: IMemberOpensearch | IMemberWithAggregatesForMergeSuggestions,
  ): string {
    if (this.isSimilarMemberFromOpensearch(member)) {
      return member.obj_attributes[MemberAttributeOpensearch.AVATAR_URL]?.string_default || null
    } else if (this.isFullMemberFromDb(member)) {
      return member.attributes[MemberAttributeName.AVATAR_URL]?.default || null
    }
    return null
  }

  static getLanguages(
    member: IMemberOpensearch | IMemberWithAggregatesForMergeSuggestions,
  ): string[] {
    if (this.isSimilarMemberFromOpensearch(member)) {
      const languages =
        member.obj_attributes[MemberAttributeOpensearch.LANGUAGES]?.string_arr_default || []
      return Array.isArray(languages) ? languages : []
    } else if (this.isFullMemberFromDb(member)) {
      const languages = member.attributes[MemberAttributeName.LANGUAGES]?.default || []
      return Array.isArray(languages) ? languages : []
    }
    return []
  }

  static getTimezone(member: IMemberOpensearch | IMemberWithAggregatesForMergeSuggestions): string {
    if (this.isSimilarMemberFromOpensearch(member)) {
      return member.obj_attributes[MemberAttributeOpensearch.TIMEZONE]?.string_default || null
    } else if (this.isFullMemberFromDb(member)) {
      return member.attributes[MemberAttributeName.TIMEZONE]?.default || null
    }
    return null
  }

  static getProgrammingLanguages(
    member: IMemberOpensearch | IMemberWithAggregatesForMergeSuggestions,
  ): string[] {
    if (this.isSimilarMemberFromOpensearch(member)) {
      return (
        member.obj_attributes[MemberAttributeOpensearch.PROGRAMMING_LANGUAGES]
          ?.string_arr_default || []
      )
    } else if (this.isFullMemberFromDb(member)) {
      return member.attributes[MemberAttributeName.PROGRAMMING_LANGUAGES]?.default || []
    }
    return []
  }

  static getOrganizations(
    member: IMemberOpensearch | IMemberWithAggregatesForMergeSuggestions,
  ): IMemberOrganizationOpensearch[] | IMemberOrganization[] {
    if (this.isSimilarMemberFromOpensearch(member)) {
      return member.nested_organizations || null
    } else if (this.isFullMemberFromDb(member)) {
      return member.organizations || null
    }
    return null
  }

  // Type guard for IMemberOpensearch
  static isSimilarMemberFromOpensearch(
    member: IMemberOpensearch | IMemberWithAggregatesForMergeSuggestions,
  ): member is IMemberOpensearch {
    return (member as IMemberOpensearch).uuid_memberId !== undefined
  }

  // Type guard for IMemberWithAggregatesForMergeSuggestions
  static isFullMemberFromDb(
    member: IMemberOpensearch | IMemberWithAggregatesForMergeSuggestions,
  ): member is IMemberWithAggregatesForMergeSuggestions {
    return (member as IMemberWithAggregatesForMergeSuggestions).id !== undefined
  }
}

export default MemberSimilarityCalculator
