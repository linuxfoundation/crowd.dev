/* eslint-disable @typescript-eslint/no-explicit-any */
import { proxyActivities } from '@temporalio/workflow'

import { IMember, MemberAttributeName, MemberIdentityType, PlatformType } from '@crowd/types'

import * as activities from '../../activities'
import { ILFIDEnrichmentGithubProfile } from '../../sources/lfid/types'

const {
  refreshToken,
  getEnrichmentLFAuth0,
  getIdentitiesExistInOtherMembers,
  updateMemberWithEnrichmentData,
  syncMembersToOpensearch,
  mergeMembers,
} = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
  retry: {
    initialInterval: '2 seconds',
    backoffCoefficient: 2,
    maximumAttempts: 3,
  },
})

export async function enrichMemberWithLFAuth0(member: IMember): Promise<void> {
  const token = await refreshToken()
  const enriched = await getEnrichmentLFAuth0(token, member)

  if (enriched) {
    console.log(`Member ${member.id} found in the lf auth0 enrichment db!`)
    let normalized: any = {}
    if (
      enriched.picture &&
      (!member.attributes?.avatarUrl || !member.attributes?.avatarUrl.default)
    ) {
      normalized.attributes = {
        ...member.attributes,
        avatarUrl: {
          default: enriched.picture,
          lfid: enriched.picture,
        },
      }
    }

    // find identities that doesn't exist in current member, and check if these identities already
    // exist in some other member
    const identitiesToCheck = []

    // email
    if (
      enriched.email &&
      !member.identities.some(
        (e) =>
          e.type === MemberIdentityType.EMAIL &&
          e.value.toLowerCase() === enriched.email.toLowerCase(),
      )
    ) {
      identitiesToCheck.push({
        type: MemberIdentityType.EMAIL,
        platform: PlatformType.LFID,
        value: enriched.email.toLowerCase(),
        verified: true,
        verifiedBy: 'lf-auth0',
      })
    }

    // lfid
    if (
      enriched.username &&
      !member.identities.some(
        (i) =>
          i.type === MemberIdentityType.USERNAME &&
          i.platform === PlatformType.LFID &&
          i.value.toLowerCase() === enriched.username.toLowerCase(),
      )
    ) {
      identitiesToCheck.push({
        platform: PlatformType.LFID,
        type: MemberIdentityType.USERNAME,
        value: enriched.username.toLowerCase(),
        verified: true,
        verifiedBy: 'lf-auth0',
      })
    }

    // github
    // check if there's a github profile in the enriched data
    const enrichmentGithub = enriched.identities.find((i) => i.provider === PlatformType.GITHUB)

    if (enrichmentGithub && enrichmentGithub.profileData) {
      if (
        enrichmentGithub.profileData.nickname &&
        !member.identities.some(
          (i) =>
            i.type === MemberIdentityType.USERNAME &&
            i.platform === PlatformType.GITHUB &&
            i.value.toLowerCase() === enrichmentGithub.profileData.nickname.toLowerCase(),
        )
      ) {
        identitiesToCheck.push({
          type: MemberIdentityType.USERNAME,
          platform: PlatformType.GITHUB,
          value: enrichmentGithub.profileData.nickname.toLowerCase(),
          verified: true,
          verifiedBy: 'lf-auth0',
        })
      }

      // also github profile might come with emails, check if these exist yet in the member
      const emailsFromGithubProfile = (
        (enrichmentGithub.profileData as ILFIDEnrichmentGithubProfile)?.emails || []
      ).filter((e) => e.verified)

      for (const githubEmail of emailsFromGithubProfile) {
        if (
          !member.identities.some(
            (e) =>
              e.type === MemberIdentityType.EMAIL &&
              e.value.toLowerCase() === githubEmail.email.toLowerCase(),
          ) &&
          // check if we haven't already added this email to identitiesToCheck
          !identitiesToCheck.some(
            (i) =>
              i.type === MemberIdentityType.EMAIL &&
              i.value.toLowerCase() === githubEmail.email.toLowerCase(),
          )
        ) {
          identitiesToCheck.push({
            platform: PlatformType.GITHUB,
            type: MemberIdentityType.EMAIL,
            value: githubEmail.email.toLowerCase(),
            verified: true,
            verifiedBy: 'lf-auth0',
          })
        }
      }

      const profileData = enrichmentGithub.profileData as ILFIDEnrichmentGithubProfile
      if (profileData.bio) {
        normalized = addAttributeToNormalizedMemberIfNotAlreadyExisting(
          member,
          normalized,
          MemberAttributeName.BIO,
          PlatformType.GITHUB,
          profileData.bio,
        )
      }

      if (profileData.hireable) {
        normalized = addAttributeToNormalizedMemberIfNotAlreadyExisting(
          member,
          normalized,
          MemberAttributeName.IS_HIREABLE,
          PlatformType.GITHUB,
          profileData.hireable,
        )
      }

      if (profileData.url) {
        normalized = addAttributeToNormalizedMemberIfNotAlreadyExisting(
          member,
          normalized,
          MemberAttributeName.URL,
          PlatformType.GITHUB,
          profileData.url,
        )
      }

      if (profileData.location) {
        normalized = addAttributeToNormalizedMemberIfNotAlreadyExisting(
          member,
          normalized,
          MemberAttributeName.LOCATION,
          PlatformType.GITHUB,
          profileData.url,
        )
      }

      // add unverified twitter identity coming from github profile
      if (
        profileData.twitter_username &&
        !member.identities.some(
          (i) =>
            i.type === MemberIdentityType.USERNAME &&
            i.platform === PlatformType.TWITTER &&
            i.value.toLowerCase() === profileData.twitter_username.toLowerCase(),
        )
      ) {
        identitiesToCheck.push({
          type: MemberIdentityType.USERNAME,
          platform: PlatformType.TWITTER,
          value: profileData.twitter_username.toLowerCase(),
          verified: false,
        })
      }
    }

    // for each identities to check, we should check if they already exist in some other member
    // if they do, we keep track of this member, remove the identity from enrichment payload
    // and after enrichment is done, merge these two members together
    const identitiesExistInOtherMembers = await getIdentitiesExistInOtherMembers(
      member.id,
      identitiesToCheck,
    )

    const identitesToAdd = identitiesToCheck.filter(
      (i) =>
        !identitiesExistInOtherMembers.some(
          (e) => e.type === i.type && e.platform === i.platform && e.value === i.value,
        ),
    )

    // update current member with enrichment data
    await updateMemberWithEnrichmentData(member.id, identitesToAdd, normalized.attributes)

    await syncMembersToOpensearch(member.id)

    const memberIdsToBeMerged: string[] = [
      ...new Set(identitiesExistInOtherMembers.map((item) => item.memberId)),
    ]

    for (const memberIdToBeMerged of memberIdsToBeMerged) {
      console.log(`${memberIdToBeMerged} will be merged with ${member.id}`)
      await mergeMembers(member.id, memberIdToBeMerged)
    }
  }
}

function addAttributeToNormalizedMemberIfNotAlreadyExisting(
  member: IMember,
  normalized: any,
  attributeName: string,
  platform: string,
  value: number | string | boolean,
) {
  if (!member.attributes?.[attributeName] && !member.attributes?.[attributeName]?.default) {
    if (!normalized.attributes) {
      normalized.attributes = {
        ...member.attributes,
        [attributeName]: {
          default: value,
          [platform]: value,
        },
      }
    } else {
      normalized.attributes = {
        ...member.attributes,
        ...normalized.attributes,
        [attributeName]: {
          default: value,
          [platform]: value,
        },
      }
    }
  }
  return normalized
}
