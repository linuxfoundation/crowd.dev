import {
  DiscussionNoteSchema,
  ExpandedCommitSchema,
  IssueNoteSchema,
  IssueSchema,
  MergeRequestNoteSchema,
  MergeRequestSchema,
  ProjectSchema,
  ProjectStarrerSchema,
  UserSchema,
} from '@gitbeaker/rest'

import {
  IActivityData,
  IMemberData,
  MemberAttributeName,
  MemberIdentityType,
  PlatformType,
} from '@crowd/types'

import { generateSourceIdHash } from '../../helpers'
import { IProcessDataContext, ProcessDataHandler } from '../../types'

import { GITLAB_GRID } from './grid'
import {
  GitlabActivityType,
  GitlabApiData,
  GitlabIssueCommentWebhook,
  GitlabIssueWebhook,
  GitlabMergeRequestCommentWebhook,
  GitlabMergeRequestWebhook,
  GitlabWebhookType,
} from './types'

const parseUser = ({ data }: { data: UserSchema }): IMemberData | undefined => {
  if (!data) {
    return undefined
  }

  const member: IMemberData = {
    identities: [
      {
        platform: PlatformType.GITLAB,
        value: data.username as string,
        type: MemberIdentityType.USERNAME,
        verified: true,
        verifiedBy: PlatformType.GITLAB,
      },
      ...(data.public_email
        ? [
            {
              platform: PlatformType.GITLAB,
              value: data.public_email,
              type: MemberIdentityType.EMAIL,
              verified: true,
              verifiedBy: PlatformType.GITLAB,
            },
          ]
        : []),
      ...(data.twitter
        ? [
            {
              platform: PlatformType.TWITTER,
              value: data.twitter,
              type: MemberIdentityType.USERNAME,
              verified: false,
            },
          ]
        : []),
      ...(data.linkedin
        ? [
            {
              platform: PlatformType.LINKEDIN,
              value: data.linkedin,
              type: MemberIdentityType.USERNAME,
              verified: false,
            },
          ]
        : []),
    ],
    displayName: data.name,
    attributes: {
      [MemberAttributeName.URL]: {
        [PlatformType.GITLAB]: data.web_url || '',
      },
      [MemberAttributeName.AVATAR_URL]: {
        [PlatformType.GITLAB]: data.avatar_url || '',
      },
      [MemberAttributeName.BIO]: {
        [PlatformType.GITLAB]: data.bio || '',
      },
      [MemberAttributeName.LOCATION]: {
        [PlatformType.GITLAB]: data.location || '',
      },
      [MemberAttributeName.WEBSITE_URL]: {
        [PlatformType.GITLAB]: data.website_url || '',
      },
      [MemberAttributeName.JOB_TITLE]: {
        [PlatformType.GITLAB]: data.job_title || '',
      },
    },
  }

  if (data.bot) {
    member.attributes[MemberAttributeName.IS_BOT] = {
      [PlatformType.GITLAB]: true,
    }
  }

  return member
}

const parseUserFromCommit = ({ data }: { data: ExpandedCommitSchema }): IMemberData => {
  const member: IMemberData = {
    identities: [
      {
        platform: PlatformType.GITLAB,
        value: data.author_email as string,
        type: MemberIdentityType.USERNAME,
        verified: false,
      },
      {
        platform: PlatformType.GITLAB,
        value: data.author_email as string,
        type: MemberIdentityType.EMAIL,
        verified: true,
        verifiedBy: PlatformType.GITLAB,
      },
    ],
    displayName: data.author_name,
    attributes: {},
  }

  return member
}

const parseIssueOpened = ({
  data,
  user,
  pathWithNamespace,
}: {
  data: IssueSchema
  projectId: string
  user: IMemberData
  pathWithNamespace: string
}): IActivityData => {
  return {
    type: GitlabActivityType.ISSUE_OPENED,
    member: user,
    timestamp: new Date(data.created_at).toISOString(),
    sourceId: data.id.toString(),
    url: data.web_url,
    title: data.title,
    body: data.description,
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.ISSUE_OPENED].score,
  }
}

const parseIssueClosed = ({
  data,
  user,
  pathWithNamespace,
}: {
  data: IssueSchema
  projectId: string
  user: IMemberData
  pathWithNamespace: string
}): IActivityData => {
  return {
    type: GitlabActivityType.ISSUE_CLOSED,
    member: user,
    timestamp: new Date(data.closed_at).toISOString(),
    // GITLAB_ISSUE_CLOSED
    sourceId: `gen-GLIC_${data.id}_${user.identities[0].value}_${new Date(
      data.closed_at,
    ).toISOString()}`,
    sourceParentId: data.id.toString(),
    url: data.web_url,
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.ISSUE_CLOSED].score,
  }
}

const parseIssueComment = ({
  data,
  user,
  pathWithNamespace,
}: {
  data: IssueNoteSchema | DiscussionNoteSchema
  projectId: string
  user: IMemberData
  pathWithNamespace: string
}): IActivityData => {
  return {
    type: GitlabActivityType.ISSUE_COMMENT,
    member: user,
    timestamp: new Date(data.created_at).toISOString(),
    sourceId: data.id.toString(),
    sourceParentId: data.noteable_id.toString(),
    body: data.body,
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.ISSUE_COMMENT].score,
  }
}

const parseMergeRequestOpened = ({
  data,
  user,
  pathWithNamespace,
}: {
  data: MergeRequestSchema
  projectId: string
  user: IMemberData
  pathWithNamespace: string
}): IActivityData => {
  return {
    type: GitlabActivityType.MERGE_REQUEST_OPENED,
    member: user,
    timestamp: new Date(data.created_at).toISOString(),
    sourceId: data.id.toString(),
    url: data.web_url,
    title: data.title,
    body: data.description,
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.MERGE_REQUEST_OPENED].score,
  }
}

const parseMergeRequestClosed = ({
  data,
  user,
  pathWithNamespace,
}: {
  data: MergeRequestSchema
  projectId: string
  user: IMemberData
  pathWithNamespace: string
}): IActivityData => {
  return {
    type: GitlabActivityType.MERGE_REQUEST_CLOSED,
    member: user,
    timestamp: new Date(data.closed_at).toISOString(),
    // GITLAB_MERGE_REQUEST_CLOSED
    sourceId: `gen-GLMRC_${data.id}_${user.identities[0].value}_${new Date(
      data.closed_at,
    ).toISOString()}`,
    sourceParentId: data.id.toString(),
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.MERGE_REQUEST_CLOSED].score,
  }
}

const parseMergeRequestMerged = ({
  data,
  user,
  pathWithNamespace,
}: {
  data: MergeRequestSchema
  projectId: string
  user: IMemberData
  pathWithNamespace: string
}): IActivityData => {
  return {
    type: GitlabActivityType.MERGE_REQUEST_MERGED,
    member: user,
    timestamp: new Date(data.merged_at).toISOString(),
    // GITLAB_MERGE_REQUEST_MERGED
    sourceId: `gen-GLMRM_${data.id}_${user.identities[0].value}_${new Date(
      data.merged_at,
    ).toISOString()}`,
    sourceParentId: data.id.toString(),
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.MERGE_REQUEST_MERGED].score,
  }
}

const parseMergeRequestComment = ({
  data,
  user,
  pathWithNamespace,
}: {
  data: MergeRequestNoteSchema | DiscussionNoteSchema
  projectId: string
  user: IMemberData
  pathWithNamespace: string
}): IActivityData => {
  return {
    type: GitlabActivityType.MERGE_REQUEST_COMMENT,
    member: user,
    timestamp: new Date(data.created_at).toISOString(),
    body: data.body,
    sourceId: data.id.toString(),
    sourceParentId: data.noteable_id.toString(),
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.MERGE_REQUEST_COMMENT].score,
  }
}

const parseMergeRequestApproved = ({
  data,
  user,
  pathWithNamespace,
}: {
  data: DiscussionNoteSchema
  projectId: string
  user: IMemberData
  pathWithNamespace: string
}): IActivityData => {
  return {
    type: GitlabActivityType.MERGE_REQUEST_REVIEW_APPROVED,
    member: user,
    timestamp: new Date(data.created_at).toISOString(),
    sourceId: data.id.toString(),
    sourceParentId: data.noteable_id.toString(),
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.MERGE_REQUEST_REVIEW_APPROVED].score,
  }
}

const parseMergeRequestChangesRequested = ({
  data,
  user,
  pathWithNamespace,
}: {
  data: DiscussionNoteSchema
  projectId: string
  user: IMemberData
  pathWithNamespace: string
}): IActivityData => {
  return {
    type: GitlabActivityType.MERGE_REQUEST_REVIEW_CHANGES_REQUESTED,
    member: user,
    timestamp: new Date(data.created_at).toISOString(),
    sourceId: data.id.toString(),
    sourceParentId: data.noteable_id.toString(),
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.MERGE_REQUEST_REVIEW_CHANGES_REQUESTED].score,
  }
}

const parseMergeRequestReviewRequested = ({
  data,
  user,
  relatedUser,
  pathWithNamespace,
}: {
  data: DiscussionNoteSchema
  projectId: string
  user: IMemberData
  relatedUser: IMemberData
  pathWithNamespace: string
}): IActivityData => {
  return {
    type: GitlabActivityType.MERGE_REQUEST_REVIEW_REQUESTED,
    member: user,
    objectMember: relatedUser,
    timestamp: new Date(data.created_at).toISOString(),
    sourceId: data.id.toString(),
    sourceParentId: data.noteable_id.toString(),
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.MERGE_REQUEST_REVIEW_REQUESTED].score,
  }
}

const parseMergeRequestAssigned = ({
  data,
  user,
  relatedUser,
  pathWithNamespace,
}: {
  data: DiscussionNoteSchema
  projectId: string
  user: IMemberData
  relatedUser: IMemberData
  pathWithNamespace: string
}): IActivityData => {
  return {
    type: GitlabActivityType.MERGE_REQUEST_ASSIGNED,
    member: user,
    objectMember: relatedUser,
    timestamp: new Date(data.created_at).toISOString(),
    sourceId: data.id.toString(),
    sourceParentId: data.noteable_id.toString(),
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.MERGE_REQUEST_ASSIGNED].score,
  }
}

const parseStar = ({
  data,
  user,
  pathWithNamespace,
}: {
  data: ProjectStarrerSchema
  projectId: string
  user: IMemberData
  pathWithNamespace: string
}): IActivityData => {
  return {
    type: GitlabActivityType.STAR,
    member: user,
    timestamp: new Date(data.starred_since).toISOString(),
    sourceId: generateSourceIdHash(
      data.user.id.toString(),
      GitlabActivityType.STAR,
      data.starred_since,
      PlatformType.GITLAB,
    ),
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.STAR].score,
  }
}

const parseFork = ({
  data,
  projectId,
  user,
  pathWithNamespace,
}: {
  data: ProjectSchema
  projectId: string
  user: IMemberData
  pathWithNamespace: string
}): IActivityData => {
  return {
    type: GitlabActivityType.FORK,
    member: user,
    timestamp: new Date(data.created_at).toISOString(),
    sourceId: data.id.toString(),
    sourceParentId: projectId,
    url: data.web_url,
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.FORK].score,
  }
}

const parseAuthoredCommit = ({
  data,
  pathWithNamespace,
  relatedData,
}: {
  data: ExpandedCommitSchema
  projectId: string
  pathWithNamespace: string
  relatedData: {
    mergeRequestId: number
  }
}): IActivityData => {
  const user = parseUserFromCommit({ data })
  return {
    type: GitlabActivityType.AUTHORED_COMMIT,
    member: user,
    timestamp: new Date(data.authored_date).toISOString(),
    sourceId: data.id,
    sourceParentId: relatedData.mergeRequestId.toString(),
    url: data.web_url,
    title: data.title,
    body: data.message,
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.AUTHORED_COMMIT].score,
    attributes: {
      insertions: data.stats.additions,
      deletions: data.stats.deletions,
      lines: data.stats.total,
      isMerge: data.parent_ids.length > 0,
    },
  }
}

const handleIssueOpenedOrUpdated = async ({
  ctx,
  data,
  user,
  pathWithNamespace,
}: {
  ctx: IProcessDataContext
  data: GitlabIssueWebhook
  projectId: string
  user: IMemberData
  pathWithNamespace: string
}): Promise<void> => {
  const activity: IActivityData = {
    type: GitlabActivityType.ISSUE_OPENED,
    member: user,
    timestamp: new Date(data.object_attributes.created_at).toISOString(),
    sourceId: data.object_attributes.id.toString(),
    url: data.object_attributes.url,
    title: data.object_attributes.title,
    body: data.object_attributes.description,
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.ISSUE_OPENED].score,
  }

  await ctx.publishActivity(activity)
}

const handleIssueClosed = async ({
  ctx,
  data,
  user,
  pathWithNamespace,
}: {
  ctx: IProcessDataContext
  data: GitlabIssueWebhook
  projectId: string
  user: IMemberData
  pathWithNamespace: string
}): Promise<void> => {
  const activity: IActivityData = {
    type: GitlabActivityType.ISSUE_CLOSED,
    member: user,
    timestamp: new Date(data.object_attributes.closed_at).toISOString(),
    // GITLAB_ISSUE_CLOSED
    sourceId: `gen-GLIC_${data.object_attributes.id}_${user.identities[0].value}_${new Date(
      data.object_attributes.closed_at,
    ).toISOString()}`,
    sourceParentId: data.object_attributes.id.toString(),
    url: data.object_attributes.url,
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.ISSUE_CLOSED].score,
  }

  await ctx.publishActivity(activity)
}

const handleIssueCommentWebhook = async ({
  ctx,
  data,
  user,
  pathWithNamespace,
}: {
  ctx: IProcessDataContext
  data: GitlabIssueCommentWebhook
  projectId: string
  user: IMemberData
  pathWithNamespace: string
}): Promise<void> => {
  const activity: IActivityData = {
    type: GitlabActivityType.ISSUE_COMMENT,
    member: user,
    timestamp: new Date(data.object_attributes.created_at).toISOString(),
    sourceId: data.object_attributes.id.toString(),
    sourceParentId: data.object_attributes.noteable_id.toString(),
    body: data.object_attributes.note,
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.ISSUE_COMMENT].score,
  }

  await ctx.publishActivity(activity)
}

const handleMergeRequestOpened = async ({
  ctx,
  data,
  user,
  pathWithNamespace,
}: {
  ctx: IProcessDataContext
  data: GitlabMergeRequestWebhook
  projectId: string
  user: IMemberData
  pathWithNamespace: string
}): Promise<void> => {
  const activity: IActivityData = {
    type: GitlabActivityType.MERGE_REQUEST_OPENED,
    member: user,
    timestamp: new Date(data.object_attributes.created_at).toISOString(),
    sourceId: data.object_attributes.id.toString(),
    url: data.object_attributes.url,
    title: data.object_attributes.title,
    body: data.object_attributes.description,
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.MERGE_REQUEST_OPENED].score,
  }

  await ctx.publishActivity(activity)
}

const handleMergeRequestClosed = async ({
  ctx,
  data,
  user,
  pathWithNamespace,
}: {
  ctx: IProcessDataContext
  data: GitlabMergeRequestWebhook
  projectId: string
  user: IMemberData
  pathWithNamespace: string
}): Promise<void> => {
  const activity: IActivityData = {
    type: GitlabActivityType.MERGE_REQUEST_CLOSED,
    member: user,
    timestamp: new Date(data.object_attributes.updated_at).toISOString(),
    // GITLAB_MERGE_REQUEST_CLOSED
    sourceId: `gen-GLMRC_${data.object_attributes.id}_${user.identities[0].value}_${new Date(
      data.object_attributes.updated_at,
    ).toISOString()}`,
    sourceParentId: data.object_attributes.id.toString(),
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.MERGE_REQUEST_CLOSED].score,
  }

  await ctx.publishActivity(activity)
}

const handleMergeRequestMerged = async ({
  ctx,
  data,
  user,
  pathWithNamespace,
}: {
  ctx: IProcessDataContext
  data: GitlabMergeRequestWebhook
  projectId: string
  user: IMemberData
  pathWithNamespace: string
}): Promise<void> => {
  const activity: IActivityData = {
    type: GitlabActivityType.MERGE_REQUEST_MERGED,
    member: user,
    timestamp: new Date(data.object_attributes.updated_at).toISOString(),
    // GITLAB_MERGE_REQUEST_MERGED
    sourceId: `gen-GLMRM_${data.object_attributes.id}_${user.identities[0].value}_${new Date(
      data.object_attributes.updated_at,
    ).toISOString()}`,
    sourceParentId: data.object_attributes.id.toString(),
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.MERGE_REQUEST_MERGED].score,
  }

  await ctx.publishActivity(activity)
}

const handleMergeRequestCommentWebhook = async ({
  ctx,
  data,
  user,
  pathWithNamespace,
}: {
  ctx: IProcessDataContext
  data: GitlabMergeRequestCommentWebhook
  projectId: string
  user: IMemberData
  pathWithNamespace: string
}): Promise<void> => {
  const activity: IActivityData = {
    type: GitlabActivityType.MERGE_REQUEST_COMMENT,
    member: user,
    timestamp: new Date(data.object_attributes.created_at).toISOString(),
    body: data.object_attributes.note,
    sourceId: data.object_attributes.id.toString(),
    sourceParentId: data.object_attributes.noteable_id.toString(),
    channel: `https://gitlab.com/${pathWithNamespace}`,
    score: GITLAB_GRID[GitlabActivityType.MERGE_REQUEST_COMMENT].score,
  }

  await ctx.publishActivity(activity)
}

const handler: ProcessDataHandler = async (ctx) => {
  //eslint-disable-next-line @typescript-eslint/no-explicit-any
  const apiData = ctx.data as GitlabApiData<any>
  const {
    type,
    data: { data, user, relatedUser, relatedData },
    projectId,
    pathWithNamespace,
    isWebhook,
  } = apiData

  let activity: IActivityData
  let relatedMember: IMemberData | undefined

  const member = parseUser({
    data: user,
  })

  if (relatedUser) {
    relatedMember = parseUser({
      data: relatedUser,
    })
  }

  if (!isWebhook) {
    switch (type) {
      case GitlabActivityType.ISSUE_OPENED:
        activity = parseIssueOpened({
          data: data as IssueSchema,
          projectId,
          user: member,
          pathWithNamespace,
        })
        break
      case GitlabActivityType.ISSUE_CLOSED:
        activity = parseIssueClosed({
          data: data as IssueSchema,
          projectId,
          user: member,
          pathWithNamespace,
        })
        break
      case GitlabActivityType.ISSUE_COMMENT:
        activity = parseIssueComment({
          data: data as IssueNoteSchema,
          projectId,
          user: member,
          pathWithNamespace,
        })
        break
      case GitlabActivityType.MERGE_REQUEST_OPENED:
        activity = parseMergeRequestOpened({
          data: data as MergeRequestSchema,
          projectId,
          user: member,
          pathWithNamespace,
        })
        break
      case GitlabActivityType.MERGE_REQUEST_CLOSED:
        activity = parseMergeRequestClosed({
          data: data as MergeRequestSchema,
          projectId,
          user: member,
          pathWithNamespace,
        })
        break
      case GitlabActivityType.MERGE_REQUEST_MERGED:
        activity = parseMergeRequestMerged({
          data: data as MergeRequestSchema,
          projectId,
          user: member,
          pathWithNamespace,
        })
        break
      case GitlabActivityType.MERGE_REQUEST_COMMENT:
        activity = parseMergeRequestComment({
          data: data as MergeRequestNoteSchema,
          projectId,
          user: member,
          pathWithNamespace,
        })
        break
      case GitlabActivityType.MERGE_REQUEST_REVIEW_APPROVED:
        activity = parseMergeRequestApproved({
          data: data as DiscussionNoteSchema,
          projectId,
          user: member,
          pathWithNamespace,
        })
        break
      case GitlabActivityType.MERGE_REQUEST_REVIEW_CHANGES_REQUESTED:
        activity = parseMergeRequestChangesRequested({
          data: data as DiscussionNoteSchema,
          projectId,
          user: member,
          pathWithNamespace,
        })
        break
      case GitlabActivityType.MERGE_REQUEST_REVIEW_REQUESTED:
        activity = parseMergeRequestReviewRequested({
          data: data as DiscussionNoteSchema,
          projectId,
          user: member,
          relatedUser: relatedMember,
          pathWithNamespace,
        })
        break
      case GitlabActivityType.MERGE_REQUEST_ASSIGNED:
        activity = parseMergeRequestAssigned({
          data: data as DiscussionNoteSchema,
          projectId,
          user: member,
          relatedUser: relatedMember,
          pathWithNamespace,
        })
        break
      case GitlabActivityType.STAR:
        activity = parseStar({
          data: data as ProjectStarrerSchema,
          projectId,
          user: member,
          pathWithNamespace,
        })
        break
      case GitlabActivityType.FORK:
        activity = parseFork({
          data: data as ProjectSchema,
          projectId,
          user: member,
          pathWithNamespace,
        })
        break
      case GitlabActivityType.AUTHORED_COMMIT:
        activity = parseAuthoredCommit({
          data: data.data,
          projectId,
          pathWithNamespace,
          relatedData: relatedData as {
            mergeRequestId: number
          },
        })
        break
      default:
        throw new Error(`Unsupported activity type: ${type}`)
    }
    await ctx.publishActivity(activity)
  } else {
    switch (type) {
      case GitlabWebhookType.ISSUE_OPENED_OR_UPDATED:
        await handleIssueOpenedOrUpdated({
          ctx,
          data: data as GitlabIssueWebhook,
          projectId,
          user: member,
          pathWithNamespace,
        })
        break
      case GitlabWebhookType.ISSUE_CLOSED:
        await handleIssueClosed({
          ctx,
          data: data as GitlabIssueWebhook,
          projectId,
          user: member,
          pathWithNamespace,
        })
        break
      case GitlabWebhookType.ISSUE_COMMENT:
        await handleIssueCommentWebhook({
          ctx,
          data: data as GitlabIssueCommentWebhook,
          projectId,
          user: member,
          pathWithNamespace,
        })
        break
      case GitlabWebhookType.MERGE_REQUEST_OPENED:
        await handleMergeRequestOpened({
          ctx,
          data: data as GitlabMergeRequestWebhook,
          projectId,
          user: member,
          pathWithNamespace,
        })
        break
      case GitlabWebhookType.MERGE_REQUEST_CLOSED:
        await handleMergeRequestClosed({
          ctx,
          data: data as GitlabMergeRequestWebhook,
          projectId,
          user: member,
          pathWithNamespace,
        })
        break
      case GitlabWebhookType.MERGE_REQUEST_MERGED:
        await handleMergeRequestMerged({
          ctx,
          data: data as GitlabMergeRequestWebhook,
          projectId,
          user: member,
          pathWithNamespace,
        })
        break
      case GitlabWebhookType.MERGE_REQUEST_COMMENT:
        await handleMergeRequestCommentWebhook({
          ctx,
          data: data as GitlabMergeRequestCommentWebhook,
          projectId,
          user: member,
          pathWithNamespace,
        })
        break
      default:
        throw new Error(`Unsupported Gitlab webhook type: ${type}`)
    }
  }
}

export default handler
