import sanitizeHtml from 'sanitize-html'

import { IMemberData, MemberAttributeName, MemberIdentityType, PlatformType } from '@crowd/types'

import { IProcessDataContext, ProcessDataHandler } from '../../types'

import {
  ILinkedInMember,
  ILinkedInOrganization,
  ILinkedInPostComment,
  ILinkedInPostReaction,
} from './api/types'
import { LINKEDIN_GRID } from './grid'
import {
  ILinkedInAuthor,
  ILinkedInCachedMember,
  ILinkedInCachedOrganization,
  ILinkedInChildCommentData,
  ILinkedInCommentData,
  ILinkedInData,
  ILinkedInReactionData,
  LinkedinActivityType,
  LinkedinStreamType,
} from './types'
import { isPrivateLinkedInMember } from './utils'

const getMember = async (
  author: ILinkedInAuthor,
  ctx: IProcessDataContext,
): Promise<IMemberData> => {
  let username: string
  let sourceId: string
  let displayName: string

  const attributes: Record<string, unknown> = {}

  if (author.type === 'user') {
    const user = author.data as ILinkedInMember
    const userId = (user as ILinkedInCachedMember).userId

    if (user.id === 'private') {
      username = `private-${userId}`
      sourceId = userId
      displayName = `Unknown #${userId}`
    } else {
      username = `${user.vanityName}`
      sourceId = user.id
      attributes[MemberAttributeName.URL] = {
        [PlatformType.LINKEDIN]: `https://www.linkedin.com/in/${user.vanityName}`,
      }
      displayName = `${user.firstName} ${user.lastName}`

      if (user.profilePictureUrl) {
        attributes[MemberAttributeName.AVATAR_URL] = {
          [PlatformType.LINKEDIN]: user.profilePictureUrl,
        }
      }
    }
  } else if (author.type === 'organization') {
    const organization = author.data as ILinkedInOrganization
    const userId = (organization as ILinkedInCachedOrganization).userId

    if (organization.name === 'private') {
      username = `private-organization-${userId}`
      displayName = `Unknown organization #${userId}`
    } else {
      username = organization.name
      displayName = organization.name
      attributes[MemberAttributeName.URL] = {
        [PlatformType.LINKEDIN]: `https://www.linkedin.com/company/${organization.vanityName}`,
      }

      if (organization.profilePictureUrl) {
        attributes[MemberAttributeName.AVATAR_URL] = {
          [PlatformType.LINKEDIN]: organization.profilePictureUrl,
        }
      }
    }

    sourceId = userId
    attributes[MemberAttributeName.IS_ORGANIZATION] = {
      [PlatformType.LINKEDIN]: true,
    }
  } else {
    await ctx.abortRunWithError(`Unknown author type: ${author.type}`)
    throw new Error(`Unknown author type: ${author.type}`)
  }

  const member: IMemberData = {
    identities: [
      {
        platform: PlatformType.LINKEDIN,
        value: username,
        type: MemberIdentityType.USERNAME,
        sourceId,
        verified: true,
        verifiedBy: PlatformType.LINKEDIN,
      },
    ],
    displayName,
    attributes,
  }

  return member
}

const processReaction: ProcessDataHandler = async (ctx) => {
  const data = ctx.data as ILinkedInReactionData
  const author = data.author as ILinkedInAuthor
  const reaction = data.reaction as ILinkedInPostReaction
  const postUrnId = data.postUrnId
  const postBody = data.postBody

  const member = await getMember(author, ctx)

  await ctx.publishActivity({
    sourceId: `${postUrnId}:${reaction.reaction}:${reaction.authorUrn}`,
    type: LinkedinActivityType.REACTION,
    timestamp: new Date(reaction.timestamp).toISOString(),
    score: LINKEDIN_GRID[LinkedinActivityType.REACTION].score,
    member,
    url: `https://www.linkedin.com/feed/update/${encodeURIComponent(postUrnId)}`,
    attributes: {
      userUrl: !isPrivateLinkedInMember(member)
        ? member.attributes[MemberAttributeName.URL][PlatformType.LINKEDIN]
        : undefined,
      postUrl: `https://www.linkedin.com/feed/update/${encodeURIComponent(postUrnId)}`,
      postBody,
      reactionType: reaction.reaction,
    },
  })
}
const processComment: ProcessDataHandler = async (ctx) => {
  const data = ctx.data as ILinkedInCommentData
  const author = data.author as ILinkedInAuthor
  const comment = data.comment as ILinkedInPostComment
  const postUrnId = data.postUrnId
  const postBody = data.postBody

  const member = await getMember(author, ctx)

  await ctx.publishActivity({
    sourceId: comment.urnId,
    type: LinkedinActivityType.COMMENT,
    timestamp: new Date(comment.timestamp).toISOString(),
    score: LINKEDIN_GRID[LinkedinActivityType.COMMENT].score,
    body: sanitizeHtml(comment.comment),
    url: `https://www.linkedin.com/feed/update/${encodeURIComponent(
      comment.objectUrn,
    )}?commentUrn=${encodeURIComponent(comment.urnId.replace('urn:li:activity:', 'activity:'))}`,
    attributes: {
      userUrl: !isPrivateLinkedInMember(member)
        ? member.attributes[MemberAttributeName.URL][PlatformType.LINKEDIN]
        : undefined,
      postUrl: `https://www.linkedin.com/feed/update/${encodeURIComponent(postUrnId)}`,
      postBody,
      imageUrl: comment.imageUrl,
    },
    member,
  })

  if (comment.childComments > 0) {
    await ctx.publishStream(`${LinkedinStreamType.COMMENT_COMMENTS}-${comment.urnId}`, {
      commentUrnId: comment.urnId,
      postUrnId,
      postBody,
    })
  }
}

const processChildComment: ProcessDataHandler = async (ctx) => {
  const data = ctx.data as ILinkedInChildCommentData
  const author = data.author as ILinkedInAuthor
  const parrentCommentUrnId = data.parentCommentUrnId
  const comment = data.comment as ILinkedInPostComment
  const postUrnId = data.postUrnId
  const postBody = data.postBody

  const member = await getMember(author, ctx)

  await ctx.publishActivity({
    sourceId: comment.urnId,
    sourceParentId: parrentCommentUrnId,
    type: LinkedinActivityType.COMMENT,
    timestamp: new Date(comment.timestamp).toISOString(),
    score: LINKEDIN_GRID[LinkedinActivityType.COMMENT].score,
    body: sanitizeHtml(comment.comment),
    url: `https://www.linkedin.com/feed/update/${encodeURIComponent(
      comment.objectUrn,
    )}?commentUrn=${encodeURIComponent(
      comment.parentUrnId.replace('urn:li:activity:', 'activity:'),
    )}&replyUrn=${encodeURIComponent(comment.urnId.replace('urn:li:activity:', 'activity:'))}`,
    attributes: {
      userUrl: !isPrivateLinkedInMember(member)
        ? member.attributes[MemberAttributeName.URL][PlatformType.LINKEDIN]
        : undefined,
      postUrl: `https://www.linkedin.com/feed/update/${encodeURIComponent(postUrnId)}`,
      postBody,
      imageUrl: comment.imageUrl,
    },
    member,
  })

  if (comment.childComments > 0) {
    await ctx.abortRunWithError('LinkedIn only has one level of comments!')
    throw new Error('LinkedIn only has one level of comments!')
  }
}

const handler: ProcessDataHandler = async (ctx) => {
  const data = ctx.data as ILinkedInData
  if (data.type === 'reaction') {
    await processReaction(ctx)
  } else if (data.type === 'comment') {
    await processComment(ctx)
  } else if (data.type === 'child_comment') {
    await processChildComment(ctx)
  } else {
    await ctx.abortRunWithError(`Unknown data type: ${data.type}`)
    throw new Error(`Unknown data type: ${data.type}`)
  }
}

export default handler
