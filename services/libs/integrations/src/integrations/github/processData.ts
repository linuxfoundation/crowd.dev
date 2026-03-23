/* eslint-disable  @typescript-eslint/no-explicit-any */
// processStream.ts content
// processData.ts content
import {
  IActivityData,
  IActivityScoringGrid,
  IMemberData,
  IOrganization,
  MemberAttributeName,
  MemberIdentityType,
  OrganizationIdentityType,
  OrganizationSource,
  PlatformType,
} from '@crowd/types'

import { generateSourceIdHash } from '../../helpers'
import { IProcessDataContext, ProcessDataHandler } from '../../types'

import { GITHUB_GRID } from './grid'
import {
  GithubActivitySubType,
  GithubActivityType,
  GithubApiData,
  GithubIssue,
  GithubIssueTimelineItem,
  GithubPrepareMemberOutput,
  GithubPrepareOrgMemberOutput,
  GithubPullRequest,
  GithubPullRequestTimelineItem,
  GithubWebhookData,
  GithubWehookEvent,
  INDIRECT_FORK,
} from './types'

const IS_TEST_ENV: boolean = process.env.NODE_ENV === 'test'

const parseBotMember = (memberData: GithubPrepareMemberOutput): IMemberData => {
  const member: IMemberData = {
    identities: [
      {
        platform: PlatformType.GITHUB,
        value: memberData.memberFromApi.login,
        type: MemberIdentityType.USERNAME,
        verified: true,
        verifiedBy: PlatformType.GITHUB,
      },
    ],
    displayName: memberData.memberFromApi.login,
    attributes: {
      [MemberAttributeName.URL]: {
        [PlatformType.GITHUB]: memberData.memberFromApi?.url || '',
      },
      [MemberAttributeName.AVATAR_URL]: {
        [PlatformType.GITHUB]: memberData.memberFromApi?.avatarUrl || '',
      },
      [MemberAttributeName.SOURCE_ID]: {
        [PlatformType.GITHUB]: memberData.memberFromApi?.id?.toString() || '',
      },
      [MemberAttributeName.IS_BOT]: {
        [PlatformType.GITHUB]: true,
      },
    },
  }

  return member
}

const parseDeletedMember = (memberData: GithubPrepareMemberOutput): IMemberData => {
  const member: IMemberData = {
    identities: [
      {
        platform: PlatformType.GITHUB,
        value: memberData.memberFromApi.login,
        type: MemberIdentityType.USERNAME,
        verified: true,
        verifiedBy: PlatformType.GITHUB,
      },
    ],
    displayName: 'Deleted User',
    attributes: {
      [MemberAttributeName.URL]: {
        [PlatformType.GITHUB]: memberData.memberFromApi?.url || '',
      },
      [MemberAttributeName.AVATAR_URL]: {
        [PlatformType.GITHUB]: memberData.memberFromApi?.avatarUrl || '',
      },
      [MemberAttributeName.BIO]: {
        [PlatformType.GITHUB]:
          "Hi, I'm @ghost! I take the place of user accounts that have been deleted. :ghost:",
      },
    },
  }

  return member
}

const parseMember = (memberData: GithubPrepareMemberOutput): IMemberData => {
  const { email, orgs, memberFromApi } = memberData

  if (memberFromApi.isBot && memberFromApi.isDeleted) {
    throw new Error('Member cannot be both bot and deleted')
  }

  if (memberFromApi.isBot) {
    return parseBotMember(memberData)
  }

  if (memberFromApi.isDeleted) {
    return parseDeletedMember(memberData)
  }

  const member: IMemberData = {
    identities: [
      {
        platform: PlatformType.GITHUB,
        value: memberFromApi.login,
        type: MemberIdentityType.USERNAME,
        sourceId: memberFromApi.id.toString(),
        verified: true,
        verifiedBy: PlatformType.GITHUB,
      },
    ],
    displayName: memberFromApi?.name?.trim() || memberFromApi.login,
    attributes: {
      [MemberAttributeName.IS_HIREABLE]: {
        [PlatformType.GITHUB]: memberFromApi.isHireable || false,
      },
      [MemberAttributeName.URL]: {
        [PlatformType.GITHUB]: memberFromApi.url,
      },
      [MemberAttributeName.BIO]: {
        [PlatformType.GITHUB]: memberFromApi.bio || '',
      },
      [MemberAttributeName.LOCATION]: {
        [PlatformType.GITHUB]: memberFromApi.location || '',
      },
      [MemberAttributeName.AVATAR_URL]: {
        [PlatformType.GITHUB]: memberFromApi.avatarUrl || '',
      },
      [MemberAttributeName.COMPANY]: {
        [PlatformType.GITHUB]: memberFromApi.company || '',
      },
    },
  }

  if (email) {
    member.identities.push({
      platform: PlatformType.GITHUB,
      value: email,
      type: MemberIdentityType.EMAIL,
      verified: true,
      verifiedBy: PlatformType.GITHUB,
    })
  }

  if (memberFromApi?.twitterUsername) {
    member.identities.push({
      platform: PlatformType.TWITTER,
      value: memberFromApi.twitterUsername,
      type: MemberIdentityType.USERNAME,
      verified: false,
    })
  }

  if (memberFromApi.websiteUrl) {
    member.attributes[MemberAttributeName.WEBSITE_URL] = {
      [PlatformType.GITHUB]: memberFromApi.websiteUrl,
    }
  }

  if (memberFromApi.company) {
    if (IS_TEST_ENV) {
      member.organizations = [
        {
          attributes: {
            name: {
              integration: ['crowd.dev'],
            },
          },
          identities: [
            {
              value: 'crowd.dev',
              platform: PlatformType.GITHUB,
              type: OrganizationIdentityType.USERNAME,
              verified: true,
            },
          ],
          source: OrganizationSource.GITHUB,
        },
      ]
    } else {
      const company = memberFromApi.company.replace('@', '').trim()

      if (orgs && company.length > 0) {
        const organizationPayload = {
          displayName: orgs.name,
          names: [orgs.name],
          identities: [
            {
              platform: PlatformType.GITHUB,
              type: OrganizationIdentityType.USERNAME,
              value: orgs.url.replace('https://github.com/', ''),
              verified: true,
            },
          ],
          description: orgs.description ?? null,
          location: orgs.location ?? null,
          logo: orgs.avatarUrl ?? null,
          source: OrganizationSource.GITHUB,
        } as IOrganization

        if (orgs.websiteUrl) {
          organizationPayload.identities.push({
            platform: PlatformType.GITHUB,
            type: OrganizationIdentityType.PRIMARY_DOMAIN,
            value: orgs.websiteUrl,
            verified: false,
          })
        }

        if (orgs.twitterUsername) {
          organizationPayload.identities.push({
            platform: PlatformType.TWITTER,
            type: OrganizationIdentityType.USERNAME,
            value: orgs.twitterUsername,
            verified: false,
          })
        }

        member.organizations = [organizationPayload]
      }
    }
  }

  // if (memberFromApi.followers && memberFromApi.followers.totalCount > 0) {
  //   member.reach = { [PlatformType.GITHUB]: memberFromApi.followers.totalCount }
  // }

  return member
}

const parseOrgMember = (memberData: GithubPrepareOrgMemberOutput): IMemberData => {
  const { orgFromApi } = memberData

  const member: IMemberData = {
    identities: [
      {
        platform: PlatformType.GITHUB,
        value: orgFromApi.login,
        type: MemberIdentityType.USERNAME,
        verified: true,
        verifiedBy: PlatformType.GITHUB,
      },
    ],
    displayName: orgFromApi?.name?.trim() || orgFromApi.login,
    attributes: {
      [MemberAttributeName.URL]: {
        [PlatformType.GITHUB]: orgFromApi.url,
      },
      [MemberAttributeName.BIO]: {
        [PlatformType.GITHUB]: orgFromApi.description || '',
      },
      [MemberAttributeName.LOCATION]: {
        [PlatformType.GITHUB]: orgFromApi.location || '',
      },
      [MemberAttributeName.AVATAR_URL]: {
        [PlatformType.GITHUB]: orgFromApi.avatarUrl || '',
      },
    },
  }

  if (orgFromApi.email) {
    member.identities.push({
      platform: PlatformType.GITHUB,
      value: orgFromApi.email,
      type: MemberIdentityType.EMAIL,
      verified: true,
      verifiedBy: PlatformType.GITHUB,
    })
  }

  if (orgFromApi?.twitterUsername) {
    member.identities.push({
      platform: PlatformType.TWITTER,
      value: orgFromApi.twitterUsername,
      type: MemberIdentityType.USERNAME,
      verified: false,
    })
  }

  if (orgFromApi.websiteUrl) {
    member.attributes[MemberAttributeName.WEBSITE_URL] = {
      [PlatformType.GITHUB]: orgFromApi.websiteUrl,
    }
  }

  // mark as organization
  member.attributes[MemberAttributeName.IS_ORGANIZATION] = {
    [PlatformType.GITHUB]: true,
  }

  return member
}

const parseStar: ProcessDataHandler = async (ctx) => {
  const apiData = ctx.data as GithubApiData
  const data = apiData.data
  const memberData = apiData.member

  const member = parseMember(memberData)

  const activity: IActivityData = {
    type: GithubActivityType.STAR,
    sourceId: generateSourceIdHash(
      data.node.login,
      GithubActivityType.STAR,
      Math.floor(new Date(data.starredAt).getTime() / 1000).toString(),
      PlatformType.GITHUB,
    ),
    sourceParentId: '',
    timestamp: new Date(data.starredAt).toISOString(),
    channel: apiData.repo.url,
    member,
    score: GITHUB_GRID.star.score,
  }

  await ctx.publishActivity(activity)
}

const parseFork: ProcessDataHandler = async (ctx) => {
  const apiData = ctx.data as GithubApiData

  if (apiData.orgMember && !apiData.member) {
    await parseForkByOrg(ctx)
    return
  } else if (apiData.member && apiData.orgMember) {
    throw new Error('Both member and orgMember are present')
  } else if (!apiData.member && !apiData.orgMember) {
    throw new Error('Both member and orgMember are missing')
  }

  const data = apiData.data
  const relatedData = apiData.relatedData
  const memberData = apiData.member
  const subType = apiData.subType

  const member = parseMember(memberData)

  if (subType && subType === INDIRECT_FORK) {
    const activity: IActivityData = {
      type: GithubActivityType.FORK,
      sourceId: data.id,
      sourceParentId: '',
      timestamp: new Date(data.createdAt).toISOString(),
      channel: apiData.repo.url,
      member,
      score: GITHUB_GRID.fork.score,
      attributes: {
        isIndirectFork: true,
        directParent: relatedData.url,
      },
    }

    await ctx.publishActivity(activity)
    return
  }

  const activity: IActivityData = {
    type: GithubActivityType.FORK,
    sourceId: data.id,
    sourceParentId: '',
    timestamp: new Date(data.createdAt).toISOString(),
    channel: apiData.repo.url,
    member,
    score: GITHUB_GRID.fork.score,
  }

  await ctx.publishActivity(activity)
}

const parseForkByOrg: ProcessDataHandler = async (ctx) => {
  const apiData = ctx.data as GithubApiData
  const data = apiData.data
  const relatedData = apiData.relatedData
  const memberData = apiData.orgMember
  const subType = apiData.subType

  const member = parseOrgMember(memberData)

  if (subType && subType === INDIRECT_FORK) {
    const activity: IActivityData = {
      type: GithubActivityType.FORK,
      sourceId: data.id,
      sourceParentId: '',
      timestamp: new Date(data.createdAt).toISOString(),
      channel: apiData.repo.url,
      member,
      score: GITHUB_GRID.fork.score,
      attributes: {
        isIndirectFork: true,
        directParent: relatedData.url,
        isForkByOrg: true,
      },
    }

    await ctx.publishActivity(activity)
    return
  }

  const activity: IActivityData = {
    type: GithubActivityType.FORK,
    sourceId: data.id,
    sourceParentId: '',
    timestamp: new Date(data.createdAt).toISOString(),
    channel: apiData.repo.url,
    member,
    score: GITHUB_GRID.fork.score,
    attributes: {
      isForkByOrg: true,
    },
  }

  await ctx.publishActivity(activity)
}

const parsePullRequestOpened: ProcessDataHandler = async (ctx) => {
  const apiData = ctx.data as GithubApiData
  const data = apiData.data as GithubPullRequest
  const memberData = apiData.member

  const member = parseMember(memberData)

  const activity: IActivityData = {
    type: GithubActivityType.PULL_REQUEST_OPENED,
    sourceId: data.id,
    sourceParentId: '',
    timestamp: new Date(data.createdAt).toISOString(),
    body: data.bodyText,
    url: data.url ? data.url : '',
    channel: apiData.repo.url,
    title: data.title,
    attributes: {
      state: data.state.toLowerCase(),
      additions: data.additions,
      deletions: data.deletions,
      changedFiles: data.changedFiles,
      authorAssociation: data.authorAssociation,
      labels: data.labels?.nodes.map((l) => l.name),
    },
    member,
    score: GITHUB_GRID[GithubActivityType.PULL_REQUEST_OPENED].score,
  }

  await ctx.publishActivity(activity)
}

const parsePullRequestClosed: ProcessDataHandler = async (ctx) => {
  const apiData = ctx.data as GithubApiData
  const data = apiData.data as GithubPullRequestTimelineItem
  const relatedData = apiData.relatedData as GithubPullRequest
  const memberData = apiData.member
  const repo = apiData.repo

  const member = parseMember(memberData)

  const activity: IActivityData = {
    type: GithubActivityType.PULL_REQUEST_CLOSED,
    sourceId: `gen-CE_${relatedData.id}_${memberData.memberFromApi.login}_${new Date(
      data.createdAt,
    ).toISOString()}`,
    sourceParentId: relatedData.id,
    timestamp: new Date(data.createdAt).toISOString(),
    body: '',
    url: relatedData.url ? relatedData.url : '',
    channel: repo.url,
    title: '',
    attributes: {
      state: relatedData.state.toLowerCase(),
      additions: relatedData.additions,
      deletions: relatedData.deletions,
      changedFiles: relatedData.changedFiles,
      authorAssociation: relatedData.authorAssociation,
      labels: relatedData.labels?.nodes.map((l) => l.name),
    },
    member,
    score: GITHUB_GRID[GithubActivityType.PULL_REQUEST_CLOSED].score,
  }

  await ctx.publishActivity(activity)
}

const parsePullRequestReviewRequested: ProcessDataHandler = async (ctx) => {
  const apiData = ctx.data as GithubApiData
  const data = apiData.data as GithubPullRequestTimelineItem
  const relatedData = apiData.relatedData as GithubPullRequest
  const memberData = apiData.member
  const objectMemberData = apiData.objectMember

  const member = parseMember(memberData)
  const objectMember = parseMember(objectMemberData)

  const subType = apiData.subType

  const sourceId =
    subType === GithubActivitySubType.PULL_REQUEST_REVIEW_REQUESTED_SINGLE
      ? `gen-RRE_${relatedData.id}_${memberData.memberFromApi.login}_${
          objectMemberData.memberFromApi.login
        }_${new Date(data.createdAt).toISOString()}`
      : `gen-RRE_${relatedData.id}_${memberData.memberFromApi.login}_${
          objectMemberData.memberFromApi.login
        }_${new Date(data.createdAt).toISOString()}`

  const activity: IActivityData = {
    type: GithubActivityType.PULL_REQUEST_REVIEW_REQUESTED,
    sourceId: sourceId,
    sourceParentId: relatedData.id,
    timestamp: new Date(data.createdAt).toISOString(),
    body: '',
    url: relatedData.url,
    channel: apiData.repo.url,
    title: '',
    attributes: {
      state: relatedData.state.toLowerCase(),
      additions: relatedData.additions,
      deletions: relatedData.deletions,
      changedFiles: relatedData.changedFiles,
      authorAssociation: relatedData.authorAssociation,
      labels: relatedData.labels?.nodes.map((l) => l.name),
    },
    member,
    objectMember,
    score: GITHUB_GRID[GithubActivityType.PULL_REQUEST_REVIEW_REQUESTED].score,
  }

  await ctx.publishActivity(activity)
}

const parsePullRequestReviewed: ProcessDataHandler = async (ctx) => {
  const apiData = ctx.data as GithubApiData
  const data = apiData.data as GithubPullRequestTimelineItem
  const relatedData = apiData.relatedData as GithubPullRequest
  const memberData = apiData.member

  const member = parseMember(memberData)

  const activity: IActivityData = {
    type: GithubActivityType.PULL_REQUEST_REVIEWED,
    sourceId: `gen-PRR_${relatedData.id}_${memberData.memberFromApi.login}_${new Date(
      data.submittedAt,
    ).toISOString()}`,
    sourceParentId: relatedData.id,
    timestamp: new Date(data.submittedAt).toISOString(),
    url: relatedData.url,
    channel: apiData.repo.url,
    body: data.body,
    title: '',
    attributes: {
      reviewState: data.state,
      state: relatedData.state.toLowerCase(),
      additions: relatedData.additions,
      deletions: relatedData.deletions,
      changedFiles: relatedData.changedFiles,
      authorAssociation: relatedData.authorAssociation,
      labels: relatedData.labels,
    },
    member,
    score: GITHUB_GRID[GithubActivityType.PULL_REQUEST_REVIEWED].score,
  }

  await ctx.publishActivity(activity)
}

const parsePullRequestAssigned: ProcessDataHandler = async (ctx) => {
  const apiData = ctx.data as GithubApiData
  const data = apiData.data as GithubPullRequestTimelineItem
  const relatedData = apiData.relatedData as GithubPullRequest
  const memberData = apiData.member
  const objectMemberData = apiData.objectMember

  const member = parseMember(memberData)
  const objectMember = parseMember(objectMemberData)

  const activity: IActivityData = {
    type: GithubActivityType.PULL_REQUEST_ASSIGNED,
    sourceId: `gen-AE_${relatedData.id}_${memberData.memberFromApi.login}_${
      objectMemberData.memberFromApi.login
    }_${new Date(data.createdAt).toISOString()}`,
    sourceParentId: relatedData.id,
    timestamp: new Date(data.createdAt).toISOString(),
    body: '',
    url: relatedData.url,
    channel: apiData.repo.url,
    title: '',
    attributes: {
      state: relatedData.state.toLowerCase(),
      additions: relatedData.additions,
      deletions: relatedData.deletions,
      changedFiles: relatedData.changedFiles,
      authorAssociation: relatedData.authorAssociation,
      labels: relatedData.labels?.nodes?.map((l) => l.name),
    },
    member,
    objectMember,
    score: GITHUB_GRID[GithubActivityType.PULL_REQUEST_ASSIGNED].score,
  }

  await ctx.publishActivity(activity)
}

const parsePullRequestMerged: ProcessDataHandler = async (ctx) => {
  const apiData = ctx.data as GithubApiData
  const data = apiData.data as GithubPullRequestTimelineItem
  const relatedData = apiData.relatedData as GithubPullRequest
  const memberData = apiData.member

  const member = parseMember(memberData)

  const activity: IActivityData = {
    type: GithubActivityType.PULL_REQUEST_MERGED,
    sourceId: `gen-ME_${relatedData.id}_${memberData.memberFromApi.login}_${new Date(
      data.createdAt,
    ).toISOString()}`,
    sourceParentId: relatedData.id,
    timestamp: new Date(data.createdAt).toISOString(),
    body: '',
    url: relatedData.url,
    channel: apiData.repo.url,
    title: '',
    attributes: {
      state: relatedData.state.toLowerCase(),
      additions: relatedData.additions,
      deletions: relatedData.deletions,
      changedFiles: relatedData.changedFiles,
      authorAssociation: relatedData.authorAssociation,
      labels: relatedData.labels?.nodes.map((l) => l.name),
    },
    member,
    score: GITHUB_GRID[GithubActivityType.PULL_REQUEST_MERGED].score,
  }

  await ctx.publishActivity(activity)
}

const parseIssueOpened: ProcessDataHandler = async (ctx) => {
  const apiData = ctx.data as GithubApiData
  const data = apiData.data as GithubIssue
  const memberData = apiData.member

  const member = parseMember(memberData)

  const activity: IActivityData = {
    type: GithubActivityType.ISSUE_OPENED,
    sourceId: data.id,
    sourceParentId: '',
    timestamp: new Date(data.createdAt).toISOString(),
    body: data.bodyText,
    url: data.url ? data.url : '',
    channel: apiData.repo.url,
    title: data.title.replace(/\0/g, ''),
    attributes: {
      state: data.state.toLowerCase(),
    },
    member,
    score: GITHUB_GRID[GithubActivityType.ISSUE_OPENED].score,
  }

  await ctx.publishActivity(activity)
}

const parseIssueClosed: ProcessDataHandler = async (ctx) => {
  const apiData = ctx.data as GithubApiData
  const data = apiData.data as GithubIssueTimelineItem
  const relatedData = apiData.relatedData as GithubIssue
  const memberData = apiData.member
  const repo = apiData.repo

  const member = parseMember(memberData)

  const activity: IActivityData = {
    type: GithubActivityType.ISSUE_CLOSED,
    sourceId: `gen-CE_${relatedData.id}_${memberData.memberFromApi.login}_${new Date(
      data.createdAt,
    ).toISOString()}`,
    sourceParentId: relatedData.id,
    timestamp: new Date(data.createdAt).toISOString(),
    body: '',
    url: relatedData.url ? relatedData.url : '',
    channel: repo.url,
    title: '',
    attributes: {
      state: relatedData.state.toLowerCase(),
    },
    member,
    score: GITHUB_GRID[GithubActivityType.ISSUE_CLOSED].score,
  }

  await ctx.publishActivity(activity)
}

const parsePullRequestComment: ProcessDataHandler = async (ctx) => {
  const apiData = ctx.data as GithubApiData
  const data = apiData.data
  const memberData = apiData.member

  const member = parseMember(memberData)

  const activity: IActivityData = {
    type: GithubActivityType.PULL_REQUEST_COMMENT,
    sourceId: data.id,
    sourceParentId: data.pullRequest.id,
    timestamp: new Date(data.createdAt).toISOString(),
    url: data.url,
    body: data.bodyText,
    channel: apiData.repo.url,
    member,
    score: GITHUB_GRID[GithubActivityType.PULL_REQUEST_COMMENT].score,
  }

  await ctx.publishActivity(activity)
}

const parsePullRequestReviewThreadComment: ProcessDataHandler = async (ctx) => {
  const apiData = ctx.data as GithubApiData
  const data = apiData.data
  const memberData = apiData.member

  const member = parseMember(memberData)

  const activity: IActivityData = {
    type: GithubActivityType.PULL_REQUEST_REVIEW_THREAD_COMMENT,
    sourceId: data.id,
    sourceParentId: data.pullRequest.id,
    timestamp: new Date(data.createdAt).toISOString(),
    body: data.bodyText,
    url: data.url,
    channel: apiData.repo.url,
    title: '',
    attributes: {
      state: data.pullRequest.state.toLowerCase(),
      additions: data.pullRequest.additions,
      deletions: data.pullRequest.deletions,
      changedFiles: data.pullRequest.changedFiles,
      authorAssociation: data.pullRequest.authorAssociation,
      labels: data.pullRequest.labels?.nodes.map((l) => l.name),
    },
    member,
    score: GITHUB_GRID[GithubActivityType.PULL_REQUEST_REVIEW_THREAD_COMMENT].score,
  }

  await ctx.publishActivity(activity)
}

const parseIssueComment: ProcessDataHandler = async (ctx) => {
  const apiData = ctx.data as GithubApiData
  const data = apiData.data
  const memberData = apiData.member

  const member = parseMember(memberData)

  const activity: IActivityData = {
    type: GithubActivityType.ISSUE_COMMENT,
    sourceId: data.id,
    sourceParentId: data.issue.id,
    timestamp: new Date(data.createdAt).toISOString(),
    url: data.url,
    body: data.bodyText,
    channel: apiData.repo.url,
    member,
    score: GITHUB_GRID[GithubActivityType.ISSUE_COMMENT].score,
  }

  await ctx.publishActivity(activity)
}

const parseDiscussionComment: ProcessDataHandler = async (ctx) => {
  const apiData = ctx.data as GithubApiData
  const data = apiData.data
  const memberData = apiData.member
  const sourceParentId = apiData.sourceParentId

  const member = parseMember(memberData)

  const subType = apiData.subType

  let activity: IActivityData

  if (subType === GithubActivitySubType.DISCUSSION_COMMENT_START) {
    activity = {
      type: GithubActivityType.DISCUSSION_COMMENT,
      sourceId: data.id,
      sourceParentId: data.discussion.id,
      timestamp: new Date(data.createdAt).toISOString(),
      url: data.url,
      body: data.bodyText,
      channel: apiData.repo.url,
      attributes: {
        isAnswer: data.isAnswer ?? undefined,
      },
      member,
      score: data.isAnswer
        ? GITHUB_GRID[GithubActivityType.DISCUSSION_COMMENT].score + 2
        : GITHUB_GRID[GithubActivityType.DISCUSSION_COMMENT].score,
    }
  } else if (subType === GithubActivitySubType.DISCUSSION_COMMENT_REPLY) {
    activity = {
      type: GithubActivityType.DISCUSSION_COMMENT,
      sourceId: data.id,
      sourceParentId,
      timestamp: new Date(data.createdAt).toISOString(),
      url: data.url,
      body: data.bodyText,
      channel: apiData.repo.url,
      member,
      score: GITHUB_GRID[GithubActivityType.DISCUSSION_COMMENT].score,
    }
  }

  await ctx.publishActivity(activity)
}

const parseAuthoredCommit: ProcessDataHandler = async (ctx) => {
  const apiData = ctx.data as GithubApiData
  const data = apiData.data
  const memberData = apiData.member

  const member = parseMember(memberData)
  const sourceParentId = apiData.sourceParentId // this is a pull request id

  const activity: IActivityData = {
    channel: apiData.repo.url,
    url: `${apiData.repo.url}/commit/${data.commit.oid}`,
    body: data.commit.message,
    type: 'authored-commit',
    sourceId: data.commit.oid,
    sourceParentId: `${sourceParentId}`,
    timestamp: new Date(data.commit.authoredDate).toISOString(),
    attributes: {
      insertions: 'additions' in data.commit ? data.commit.additions : 0,
      deletions: 'deletions' in data.commit ? data.commit.deletions : 0,
      lines:
        'additions' in data.commit && 'deletions' in data.commit
          ? data.commit.additions - data.commit.deletions
          : 0,
      isMerge: data.commit.parents.totalCount > 1,
    },
    member,
    score: GITHUB_GRID[GithubActivityType.AUTHORED_COMMIT].score,
  }

  await ctx.publishActivity(activity)
}

const parseWebhookIssue = async (ctx: IProcessDataContext) => {
  const data = ctx.data as GithubWebhookData
  const payload = data.data
  const memberData = data.member

  const member = parseMember(memberData)

  let type: GithubActivityType
  let scoreGrid: IActivityScoringGrid
  let timestamp: string
  let sourceId: string
  let sourceParentId: string
  let body = ''
  let title = ''

  switch (payload.action) {
    case 'edited':
    case 'opened':
    case 'reopened':
      type = GithubActivityType.ISSUE_OPENED
      scoreGrid = GITHUB_GRID[GithubActivityType.ISSUE_OPENED]
      timestamp = payload.issue.created_at
      sourceParentId = null
      sourceId = payload.issue.node_id.toString()
      body = payload.issue.body
      title = payload.issue.title
      break

    case 'closed':
      type = GithubActivityType.ISSUE_CLOSED
      scoreGrid = GITHUB_GRID[GithubActivityType.ISSUE_CLOSED]
      timestamp = payload.issue.closed_at
      sourceParentId = payload.issue.node_id.toString()
      sourceId = `gen-CE_${payload.issue.node_id.toString()}_${payload.sender.login}_${new Date(
        payload.issue.closed_at,
      ).toISOString()}`
      break

    default:
      return
  }

  const issue = payload.issue

  if (member) {
    const activity: IActivityData = {
      member,
      type,
      timestamp: new Date(timestamp).toISOString(),
      sourceId,
      sourceParentId,
      url: issue.html_url,
      title,
      channel: payload.repository.html_url,
      body,
      attributes: {
        state: issue.state,
      },
      score: scoreGrid.score,
    }

    await ctx.publishActivity(activity)
  }
}

const parseWebhookDiscussion = async (ctx: IProcessDataContext) => {
  const data = ctx.data as GithubWebhookData
  const payload = data.data
  const memberData = data.member

  const member = parseMember(memberData)
  if (payload.action === 'answered') {
    if (member) {
      const answer = payload.answer
      const activity: IActivityData = {
        member,
        type: GithubActivityType.DISCUSSION_COMMENT,
        timestamp: new Date(answer.created_at).toISOString(),
        sourceId: answer.node_id.toString(),
        sourceParentId: payload.discussion.node_id.toString(),
        attributes: {
          isSelectedAnswer: true,
        },
        channel: payload.repository.html_url,
        body: answer.body,
        url: answer.html_url,
        score: GITHUB_GRID[GithubActivityType.DISCUSSION_COMMENT].score + 2,
      }

      await ctx.publishActivity(activity)
    }
  }

  if (!['edited', 'created'].includes(payload.action)) {
    return
  }

  const discussion = payload.discussion

  if (member) {
    const activity: IActivityData = {
      member,
      type: GithubActivityType.DISCUSSION_STARTED,
      timestamp: new Date(discussion.created_at).toISOString(),
      sourceId: discussion.node_id.toString(),
      sourceParentId: null,
      url: discussion.html_url,
      title: discussion.title,
      channel: payload.repository.html_url,
      body: discussion.body,
      attributes: {
        category: {
          id: discussion.category.node_id,
          isAnswerable: discussion.category.is_answerable,
          name: discussion.category.name,
          slug: discussion.category.slug,
          emoji: discussion.category.emoji,
          description: discussion.category.description,
        },
      },
      score: GITHUB_GRID[GithubActivityType.DISCUSSION_STARTED].score,
    }

    await ctx.publishActivity(activity)
  }
}

const parseWebhookPullRequest = async (ctx: IProcessDataContext) => {
  const data = ctx.data as GithubWebhookData
  const payload = data.data
  const memberData = data.member

  const member = parseMember(memberData)
  const objectMemberData = data.objectMember
  const objectMember = objectMemberData ? parseMember(objectMemberData) : null

  let type: GithubActivityType
  let scoreGrid: IActivityScoringGrid
  let timestamp: string
  let sourceParentId: string
  let sourceId: string
  let body = ''
  let title = ''

  switch (payload.action) {
    case 'edited':
    case 'opened':
    case 'reopened': {
      type = GithubActivityType.PULL_REQUEST_OPENED
      scoreGrid = GITHUB_GRID[GithubActivityType.PULL_REQUEST_OPENED]
      timestamp = payload.pull_request.created_at
      sourceId = payload.pull_request.node_id.toString()
      sourceParentId = null
      body = payload.pull_request.body
      title = payload.pull_request.title
      break
    }

    case 'closed': {
      type = GithubActivityType.PULL_REQUEST_CLOSED
      scoreGrid = GITHUB_GRID[GithubActivityType.PULL_REQUEST_CLOSED]
      timestamp = payload.pull_request.closed_at
      sourceParentId = payload.pull_request.node_id.toString()
      sourceId = `gen-CE_${payload.pull_request.node_id.toString()}_${
        payload.sender.login
      }_${new Date(payload.pull_request.closed_at).toISOString()}`
      break
    }

    case 'assigned': {
      type = GithubActivityType.PULL_REQUEST_ASSIGNED
      scoreGrid = GITHUB_GRID[GithubActivityType.PULL_REQUEST_ASSIGNED]
      timestamp = payload.pull_request.updated_at
      sourceParentId = payload.pull_request.node_id.toString()
      sourceId = `gen-AE_${payload.pull_request.node_id.toString()}_${payload.sender.login}_${
        payload.assignee.login
      }_${new Date(payload.pull_request.updated_at).toISOString()}`
      break
    }

    case 'review_requested': {
      type = GithubActivityType.PULL_REQUEST_REVIEW_REQUESTED
      scoreGrid = GITHUB_GRID[GithubActivityType.PULL_REQUEST_REVIEW_REQUESTED]
      timestamp = payload.pull_request.updated_at
      sourceParentId = payload.pull_request.node_id.toString()
      sourceId = `gen-RRE_${payload.pull_request.node_id.toString()}_${payload.sender.login}_${
        payload.requested_reviewer.login
      }_${new Date(payload.pull_request.updated_at).toISOString()}`
      break
    }

    case 'merged': {
      type = GithubActivityType.PULL_REQUEST_MERGED
      scoreGrid = GITHUB_GRID[GithubActivityType.PULL_REQUEST_MERGED]
      timestamp = payload.pull_request.merged_at
      sourceParentId = payload.pull_request.node_id.toString()
      sourceId = `gen-ME_${payload.pull_request.node_id.toString()}_${
        payload.pull_request.merged_by?.login || member.identities[0].value
      }_${new Date(payload.pull_request.merged_at).toISOString()}`
      break
    }

    default: {
      return undefined
    }
  }

  const pull = payload.pull_request

  if (member) {
    const activity: IActivityData = {
      member,
      objectMember,
      type,
      timestamp: new Date(timestamp).toISOString(),
      sourceId,
      sourceParentId,
      url: pull.html_url,
      title,
      channel: payload.repository.html_url,
      body,
      score: scoreGrid.score,
      attributes: {
        state: pull.state,
        additions: pull.additions,
        deletions: pull.deletions,
        changedFiles: pull.changed_files,
        authorAssociation: pull.author_association,
        labels: pull.labels.map((l) => l.name),
      },
    }

    await ctx.publishActivity(activity)
  }
}

const parseWebhookPullRequestReview = async (ctx: IProcessDataContext) => {
  const data = ctx.data as GithubWebhookData
  const payload = data.data
  const memberData = data.member

  const member = parseMember(memberData)

  let type: GithubActivityType
  let scoreGrid: IActivityScoringGrid
  let timestamp: string
  let sourceParentId: string
  let sourceId: string
  let body = ''

  switch (payload.action) {
    case 'submitted': {
      // additional comments to existing review threads also result in submitted events
      // since these will be handled in pull_request_review_comment.created events
      // we're ignoring when state is commented and it has no body.
      if (payload.review.state === 'commented' && payload.review.body === null) {
        return undefined
      }

      type = GithubActivityType.PULL_REQUEST_REVIEWED
      scoreGrid = GITHUB_GRID[GithubActivityType.PULL_REQUEST_REVIEWED]
      timestamp = payload.review.submitted_at
      sourceParentId = payload.pull_request.node_id.toString()
      sourceId = `gen-PRR_${payload.pull_request.node_id.toString()}_${
        payload.sender.login
      }_${new Date(payload.review.submitted_at).toISOString()}`
      body = payload.review.body
      break
    }
    default: {
      return undefined
    }
  }

  const pull = payload.pull_request

  if (member) {
    const activity: IActivityData = {
      member,
      type,
      timestamp: new Date(timestamp).toISOString(),
      sourceId,
      sourceParentId,
      url: pull.html_url,
      title: '',
      channel: payload.repository.html_url,
      body,
      score: scoreGrid.score,
      attributes: {
        reviewState: (payload.review?.state as string).toUpperCase(),
        state: pull.state,
        authorAssociation: pull.author_association,
        labels: pull.labels.map((l) => l.name),
      },
    }

    await ctx.publishActivity(activity)
  }
}

const parseWebhookStar = async (ctx: IProcessDataContext) => {
  const data = ctx.data as GithubWebhookData
  const payload = data.data
  const memberData = data.member

  const member = parseMember(memberData)

  let type: GithubActivityType
  switch (payload.action) {
    case 'created': {
      type = GithubActivityType.STAR
      break
    }

    case 'deleted': {
      type = GithubActivityType.UNSTAR
      break
    }

    default: {
      return
    }
  }

  if (
    member &&
    (type === GithubActivityType.UNSTAR ||
      (type === GithubActivityType.STAR && payload.starred_at !== null))
  ) {
    const starredAt =
      type === GithubActivityType.STAR ? new Date(payload.starred_at).toISOString() : data.date

    const activity: IActivityData = {
      member,
      type,
      timestamp: starredAt,
      sourceId: generateSourceIdHash(
        payload.sender.login,
        type,
        Math.floor(new Date(starredAt).getTime() / 1000).toString(),
        PlatformType.GITHUB,
      ),
      sourceParentId: null,
      channel: payload.repository.html_url,
      score:
        type === 'star'
          ? GITHUB_GRID[GithubActivityType.STAR].score
          : GITHUB_GRID[GithubActivityType.UNSTAR].score,
    }

    await ctx.publishActivity(activity)
  }
}

const parseWebhookFork = async (ctx: IProcessDataContext) => {
  const data = ctx.data as GithubWebhookData
  const payload = data.data

  // this is org member
  if (data.orgMember && !data.member) {
    const orgMember = parseOrgMember(data.orgMember)
    if (orgMember) {
      const activity: IActivityData = {
        member: orgMember,
        type: GithubActivityType.FORK,
        timestamp: new Date(payload.forkee.created_at).toISOString(),
        sourceId: payload.forkee.node_id.toString(),
        sourceParentId: null,
        channel: payload.repository.html_url,
        score: GITHUB_GRID.fork.score,
      }

      await ctx.publishActivity(activity)
    }
    // this is member
  } else if (data.member && !data.orgMember) {
    const member = parseMember(data.member)
    if (member) {
      const activity: IActivityData = {
        member,
        type: GithubActivityType.FORK,
        timestamp: new Date(payload.forkee.created_at).toISOString(),
        sourceId: payload.forkee.node_id.toString(),
        sourceParentId: null,
        channel: payload.repository.html_url,
        score: GITHUB_GRID.fork.score,
      }

      await ctx.publishActivity(activity)
    } else if (data.member && data.orgMember) {
      throw new Error('Both member and orgMember are present in webhook fork data')
    } else if (!data.member && !data.orgMember) {
      throw new Error('Both member and orgMember are missing in webhook fork data')
    }
  }
}

const parseWebhookComment = async (ctx: IProcessDataContext) => {
  const data = ctx.data as GithubWebhookData
  const payload = data.data
  const memberData = data.member

  const member = parseMember(memberData)
  let type: GithubActivityType
  let sourceParentId: string | undefined

  switch (data.webhookType) {
    case GithubWehookEvent.DISCUSSION_COMMENT: {
      switch (payload.action) {
        case 'created':
        case 'edited':
          type = GithubActivityType.DISCUSSION_COMMENT
          sourceParentId = payload.discussion.node_id.toString()
          break
        default:
          return undefined
      }
      break
    }

    case GithubWehookEvent.ISSUE_COMMENT:
    case GithubWehookEvent.PULL_REQUEST_COMMENT: {
      switch (payload.action) {
        case 'created':
        case 'edited': {
          if ('pull_request' in payload.issue) {
            type = GithubActivityType.PULL_REQUEST_COMMENT
          } else {
            type = GithubActivityType.ISSUE_COMMENT
          }
          sourceParentId = payload.issue.node_id.toString()
          break
        }

        default:
          return
      }
      break
    }

    default: {
      return
    }
  }

  if (member) {
    const comment = payload.comment
    const activity: IActivityData = {
      member,
      type,
      timestamp: new Date(comment.created_at).toISOString(),
      sourceId: comment.node_id.toString(),
      sourceParentId,
      url: comment.html_url,
      body: comment.body,
      channel: payload.repository.html_url,
      score: GITHUB_GRID[type].score,
    }

    await ctx.publishActivity(activity)
  }
}

const parseWebhookPullRequestReviewThreadComment = async (ctx: IProcessDataContext) => {
  const data = ctx.data as GithubWebhookData
  const payload = data.data
  const memberData = data.member

  const member = parseMember(memberData)

  let type: GithubActivityType
  let scoreGrid: IActivityScoringGrid
  let timestamp: string
  let sourceParentId: string
  let sourceId: string
  let body = ''

  switch (payload.action) {
    case 'created': {
      type = GithubActivityType.PULL_REQUEST_REVIEW_THREAD_COMMENT
      scoreGrid = GITHUB_GRID[GithubActivityType.PULL_REQUEST_REVIEW_THREAD_COMMENT]
      timestamp = payload.comment.created_at
      sourceParentId = payload.pull_request.node_id
      sourceId = payload.comment.node_id
      body = payload.comment.body
      break
    }
    default: {
      return undefined
    }
  }

  if (member) {
    const activity: IActivityData = {
      member,
      type,
      timestamp: new Date(timestamp).toISOString(),
      sourceId,
      sourceParentId,
      url: payload.comment.html_url,
      title: '',
      channel: payload.repository.html_url,
      body,
      score: scoreGrid.score,
      attributes: {
        state: payload.pull_request.state,
        authorAssociation: payload.pull_request.author_association,
        labels: payload.pull_request.labels.map((l) => l.name),
      },
    }

    await ctx.publishActivity(activity)
  }
}

const parseDiscussionStarted: ProcessDataHandler = async (ctx) => {
  const apiData = ctx.data as GithubApiData
  const data = apiData.data
  const memberData = apiData.member

  const member = parseMember(memberData)

  const activity: IActivityData = {
    type: GithubActivityType.DISCUSSION_STARTED,
    sourceId: data.id,
    sourceParentId: '',
    timestamp: new Date(data.createdAt).toISOString(),
    body: data.bodyText,
    url: data.url ? data.url : '',
    channel: apiData.repo.url,
    title: data.title,
    attributes: {
      category: {
        id: data.category.id,
        isAnswerable: data.category.isAnswerable,
        name: data.category.name,
        slug: data.category.slug,
        emoji: data.category.emoji,
        description: data.category.description,
      },
    },
    member,
    score: GITHUB_GRID[GithubActivityType.DISCUSSION_STARTED].score,
  }

  await ctx.publishActivity(activity)
}

const handler: ProcessDataHandler = async (ctx) => {
  const data = ctx.data as any

  const event = data?.type as GithubActivityType
  const webhookEvent = data?.webhookType as GithubWehookEvent

  if (event) {
    // parse github api data
    switch (event) {
      case GithubActivityType.STAR:
        await parseStar(ctx)
        break
      case GithubActivityType.FORK:
        await parseFork(ctx)
        break
      case GithubActivityType.PULL_REQUEST_OPENED:
        await parsePullRequestOpened(ctx)
        break
      case GithubActivityType.PULL_REQUEST_CLOSED:
        await parsePullRequestClosed(ctx)
        break
      case GithubActivityType.PULL_REQUEST_REVIEW_REQUESTED:
        await parsePullRequestReviewRequested(ctx)
        break
      case GithubActivityType.PULL_REQUEST_REVIEWED:
        await parsePullRequestReviewed(ctx)
        break
      case GithubActivityType.PULL_REQUEST_ASSIGNED:
        await parsePullRequestAssigned(ctx)
        break
      case GithubActivityType.PULL_REQUEST_MERGED:
        await parsePullRequestMerged(ctx)
        break
      case GithubActivityType.ISSUE_OPENED:
        await parseIssueOpened(ctx)
        break
      case GithubActivityType.ISSUE_CLOSED:
        await parseIssueClosed(ctx)
        break
      case GithubActivityType.PULL_REQUEST_COMMENT:
        await parsePullRequestComment(ctx)
        break
      case GithubActivityType.PULL_REQUEST_REVIEW_THREAD_COMMENT:
        await parsePullRequestReviewThreadComment(ctx)
        break
      case GithubActivityType.ISSUE_COMMENT:
        await parseIssueComment(ctx)
        break
      case GithubActivityType.DISCUSSION_STARTED:
        await parseDiscussionStarted(ctx)
        break
      case GithubActivityType.DISCUSSION_COMMENT:
        await parseDiscussionComment(ctx)
        break
      case GithubActivityType.AUTHORED_COMMIT:
        await parseAuthoredCommit(ctx)
        break
      default:
        await ctx.abortWithError(`Event not supported '${event}'!`)
    }
  } else if (webhookEvent) {
    // TODO: implement webhook events
    switch (webhookEvent) {
      case GithubWehookEvent.ISSUES:
        await parseWebhookIssue(ctx)
        break
      case GithubWehookEvent.DISCUSSION:
        await parseWebhookDiscussion(ctx)
        break
      case GithubWehookEvent.PULL_REQUEST:
        await parseWebhookPullRequest(ctx)
        break
      case GithubWehookEvent.PULL_REQUEST_REVIEW:
        await parseWebhookPullRequestReview(ctx)
        break
      case GithubWehookEvent.STAR:
        await parseWebhookStar(ctx)
        break
      case GithubWehookEvent.FORK:
        await parseWebhookFork(ctx)
        break
      case GithubWehookEvent.DISCUSSION_COMMENT:
      case GithubWehookEvent.ISSUE_COMMENT:
      case GithubWehookEvent.PULL_REQUEST_COMMENT:
        await parseWebhookComment(ctx)
        break
      case GithubWehookEvent.PULL_REQUEST_REVIEW_COMMENT:
        await parseWebhookPullRequestReviewThreadComment(ctx)
        break
      default:
        await ctx.abortWithError(`Webhook event not supported '${webhookEvent}'!`)
    }
  }
}

export default handler
