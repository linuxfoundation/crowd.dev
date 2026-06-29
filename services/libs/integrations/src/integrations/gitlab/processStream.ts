import { Gitlab } from '@gitbeaker/rest'

import { timeout } from '@crowd/common'

import { IProcessStreamContext, ProcessStreamHandler } from '../../types'

import { getForks } from './api/getForks'
import { getIssueDiscussions } from './api/getIssueDiscussions'
import { getIssues } from './api/getIssues'
import { getMergeRequestCommits } from './api/getMergeRequestCommits'
import { getMergeRequestDiscussionsAndEvents } from './api/getMergeRequestDiscussionsAndEvents'
import { getMergeRequests } from './api/getMergeRequests'
import { getStars } from './api/getStars'
import { GITLAB_GHOST_USER_ID, getUser } from './api/getUser'
import { getUserByUsername } from './api/getUser'
import { refreshToken } from './api/refreshToken'
import {
  GitlabActivityData,
  GitlabActivityType,
  GitlabApiData,
  GitlabApiResult,
  GitlabBasicStream,
  GitlabRootStream,
  GitlabStreamType,
} from './types'

/* eslint-disable @typescript-eslint/no-explicit-any */

interface GitlabStreamHandler {
  (
    ctx: IProcessStreamContext,
    api: InstanceType<typeof Gitlab>,
    data: GitlabBasicStream,
  ): Promise<void>
}

interface GitlabRootStreamHandler {
  (ctx: IProcessStreamContext, data: GitlabRootStream): Promise<void>
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
interface GitlabProcessStreamResultHandler<T> {
  (params: {
    ctx: IProcessStreamContext
    result: GitlabApiResult<T>
    activityType: GitlabActivityType
    streamType: GitlabStreamType
    dataId: string | null
    projectId: string
    pathWithNamespace: string
    meta?: Record<string, unknown>
  }): Promise<void>
}

const handleRootStream: GitlabRootStreamHandler = async (ctx, data) => {
  // Refresh token if necessary
  if (ctx.integration.refreshToken) {
    const newTokens = await refreshToken(ctx)
    await ctx.updateIntegrationToken(newTokens.access_token)
    await ctx.updateIntegrationRefreshToken(newTokens.refresh_token)
  }

  for (const { id, path_with_namespace } of data.projects) {
    const streamTypes = ctx.onboarding
      ? [
          GitlabStreamType.ISSUES,
          GitlabStreamType.MERGE_REQUESTS,
          GitlabStreamType.STARS,
          GitlabStreamType.FORKS,
        ]
      : [GitlabStreamType.STARS, GitlabStreamType.FORKS]

    for (const streamType of streamTypes) {
      await ctx.publishStream<GitlabBasicStream>(`${streamType}:${id}:firstPage`, {
        projectId: id,
        pathWithNamespace: path_with_namespace,
        page: 1,
      })
    }
  }
}

const handleIssuesStream: GitlabStreamHandler = async (ctx, api, data) => {
  const result = await getIssues({ api, projectId: data.projectId, page: data.page, ctx })

  // issue opened
  await handleApiResult({
    ctx,
    result,
    activityType: GitlabActivityType.ISSUE_OPENED,
    streamType: GitlabStreamType.ISSUES,
    dataId: null,
    projectId: data.projectId,
    pathWithNamespace: data.pathWithNamespace,
  })

  // issue closed
  for (const item of result.data) {
    if (item.data.closed_at) {
      ctx.log.info(item)
      const user = await getUser(
        api,
        parseInt(item.data.closed_by) || (item.data.closed_by as any)?.id || GITLAB_GHOST_USER_ID,
        ctx,
      )
      await ctx.processData<GitlabApiData<typeof item.data>>({
        data: {
          data: item.data,
          user,
        },
        type: GitlabActivityType.ISSUE_CLOSED,
        projectId: data.projectId,
        pathWithNamespace: data.pathWithNamespace,
      })
    }
  }

  // issue comments
  for (const item of result.data) {
    await ctx.publishStream<GitlabBasicStream>(
      `${GitlabStreamType.ISSUE_DISCUSSIONS}:${data.projectId}:${item.data.iid}:firstPage`,
      {
        projectId: data.projectId,
        pathWithNamespace: data.pathWithNamespace,
        meta: {
          issueId: item.data.iid,
        },
        page: 1,
      },
    )
  }
}

const handleIssueDiscussionsStream: GitlabStreamHandler = async (ctx, api, data) => {
  const issueId = data?.meta?.issueId as number

  // Validate that issueId exists and is a valid number
  if (!issueId || typeof issueId !== 'number' || issueId <= 0) {
    ctx.log.error(
      {
        projectId: data.projectId,
        issueId,
        issueIdType: typeof issueId,
        meta: data?.meta,
        dataKeys: Object.keys(data || {}),
      },
      'Invalid or missing issueId in handleIssueDiscussionsStream',
    )
    // Skip processing this stream if issueId is invalid
    return
  }

  const result = await getIssueDiscussions({
    api,
    projectId: data.projectId,
    issueId,
    page: data.page,
    ctx,
  })
  await handleApiResult({
    ctx,
    result,
    activityType: GitlabActivityType.ISSUE_COMMENT,
    streamType: GitlabStreamType.ISSUE_DISCUSSIONS,
    dataId: `${data?.meta?.issueId}`,
    projectId: data.projectId,
    pathWithNamespace: data.pathWithNamespace,
    meta: data.meta,
  })
}

const handleMergeRequestsStream: GitlabStreamHandler = async (ctx, api, data) => {
  const result = await getMergeRequests({ api, projectId: data.projectId, page: data.page, ctx })

  // Opened
  await handleApiResult({
    ctx,
    result,
    activityType: GitlabActivityType.MERGE_REQUEST_OPENED,
    streamType: GitlabStreamType.MERGE_REQUESTS,
    dataId: null,
    projectId: data.projectId,
    pathWithNamespace: data.pathWithNamespace,
  })

  for (const item of result.data) {
    // Merged
    if (item.data.merged_at) {
      const user = await getUser(api, item.data.merged_by?.id || GITLAB_GHOST_USER_ID, ctx)
      await ctx.processData<GitlabApiData<typeof item.data>>({
        data: {
          data: item.data,
          user,
        },
        type: GitlabActivityType.MERGE_REQUEST_MERGED,
        projectId: data.projectId,
        pathWithNamespace: data.pathWithNamespace,
      })
    }

    // Closed
    if (item.data.closed_at) {
      const user = await getUser(api, item.data.closed_by?.id || GITLAB_GHOST_USER_ID, ctx)
      await ctx.processData<GitlabApiData<typeof item.data>>({
        data: {
          data: item.data,
          user,
        },
        type: GitlabActivityType.MERGE_REQUEST_CLOSED,
        projectId: data.projectId,
        pathWithNamespace: data.pathWithNamespace,
      })
    }

    // comments and events stream
    await ctx.publishStream<GitlabBasicStream>(
      `${GitlabStreamType.MERGE_REQUEST_DISCUSSIONS_AND_EVENTS}:${data.projectId}:${item.data.id}:firstPage`,
      {
        projectId: data.projectId,
        pathWithNamespace: data.pathWithNamespace,
        meta: {
          mergeRequestIId: item.data.iid,
          mergeRequestId: item.data.id,
        },
        page: 1,
      },
    )

    await timeout(3000)
    // Add this new stream for merge request commits
    await ctx.publishStream<GitlabBasicStream>(
      `${GitlabStreamType.MERGE_REQUEST_COMMITS}:${data.projectId}:${item.data.id}:firstPage`,
      {
        projectId: data.projectId,
        pathWithNamespace: data.pathWithNamespace,
        meta: {
          mergeRequestIId: item.data.iid,
          mergeRequestId: item.data.id,
        },
        page: 1,
      },
    )
  }
}

export const handleMergeRequestDiscussionsAndEvents: GitlabStreamHandler = async (
  ctx,
  api,
  data,
) => {
  const result = await getMergeRequestDiscussionsAndEvents({
    api,
    projectId: data.projectId,
    mergeRequestIId: data?.meta?.mergeRequestIId as number,
    page: data.page,
    ctx,
  })

  for (const item of result.data) {
    // system notes
    if (item.data.system) {
      const note = item.data
      if (note.body.startsWith('assigned to @')) {
        const assignedMatches = note.body.match(/assigned to @(\w+)/g)
        if (assignedMatches) {
          const assignees = assignedMatches.map((match) => match.split('@')[1])
          const relatedUser = await getUserByUsername(
            ctx.integration.token as string,
            assignees[0],
            ctx,
          )
          if (relatedUser) {
            await ctx.processData<GitlabApiData<typeof item.data>>({
              data: {
                data: item.data,
                user: item.user,
                relatedUser,
              },
              type: GitlabActivityType.MERGE_REQUEST_ASSIGNED,
              projectId: data.projectId,
              pathWithNamespace: data.pathWithNamespace,
            })
          }
        }
      } else if (note.body.startsWith('requested review from @')) {
        const reviewerMatches = note.body.match(/requested review from @(\w+)/g)
        if (reviewerMatches) {
          const reviewers = reviewerMatches.map((match) => match.split('@')[1])
          const relatedUser = await getUserByUsername(
            ctx.integration.token as string,
            reviewers[0],
            ctx,
          )

          if (relatedUser) {
            await ctx.processData<GitlabApiData<typeof item.data>>({
              data: {
                data: item.data,
                user: item.user,
                relatedUser,
              },
              type: GitlabActivityType.MERGE_REQUEST_REVIEW_REQUESTED,
              projectId: data.projectId,
              pathWithNamespace: data.pathWithNamespace,
            })
          }
        }
        //
      } else if (note.body.startsWith('approved this merge request')) {
        await ctx.processData<GitlabApiData<typeof item.data>>({
          data: {
            data: item.data,
            user: item.user,
          },
          type: GitlabActivityType.MERGE_REQUEST_REVIEW_APPROVED,
          projectId: data.projectId,
          pathWithNamespace: data.pathWithNamespace,
        })
      } else if (note.body.startsWith('requested changes')) {
        await ctx.processData<GitlabApiData<typeof item.data>>({
          data: {
            data: item.data,
            user: item.user,
          },
          type: GitlabActivityType.MERGE_REQUEST_REVIEW_CHANGES_REQUESTED,
          projectId: data.projectId,
          pathWithNamespace: data.pathWithNamespace,
        })
      } else {
        ctx.log.debug(`Unhandled merge request event: ${note.body}`)
      }
    } else if (!item.data.system) {
      // comments
      await ctx.processData<GitlabApiData<typeof item.data>>({
        data: {
          data: item.data,
          user: item.user,
        },
        type: GitlabActivityType.MERGE_REQUEST_COMMENT,
        projectId: data.projectId,
        pathWithNamespace: data.pathWithNamespace,
      })
    }
  }

  if (result.nextPage) {
    await ctx.publishStream<GitlabBasicStream>(
      `${GitlabStreamType.MERGE_REQUEST_DISCUSSIONS_AND_EVENTS}:${data.projectId}:${data?.meta?.mergeRequestId}:${result.nextPage}`,
      {
        projectId: data.projectId,
        pathWithNamespace: data.pathWithNamespace,
        page: result.nextPage,
        meta: {
          mergeRequestIId: data?.meta?.mergeRequestIId,
          mergeRequestId: data?.meta?.mergeRequestId,
        },
      },
    )
  }
}

export const handleMergeRequestCommitsStream: GitlabStreamHandler = async (ctx, api, data) => {
  const result = await getMergeRequestCommits({
    api,
    projectId: data.projectId,
    mergeRequestIId: data?.meta?.mergeRequestIId as number,
    page: data.page,
    ctx,
  })

  for (const commit of result.data) {
    await ctx.processData<GitlabApiData<typeof commit>>({
      data: {
        data: commit,
        relatedData: {
          mergeRequestIId: data?.meta?.mergeRequestIId,
          mergeRequestId: data?.meta?.mergeRequestId,
        },
      },
      type: GitlabActivityType.AUTHORED_COMMIT,
      projectId: data.projectId,
      pathWithNamespace: data.pathWithNamespace,
    })
  }

  if (result.nextPage) {
    await ctx.publishStream<GitlabBasicStream>(
      `${GitlabStreamType.MERGE_REQUEST_COMMITS}:${data.projectId}:${data?.meta?.mergeRequestId}:${result.nextPage}`,
      {
        projectId: data.projectId,
        pathWithNamespace: data.pathWithNamespace,
        meta: {
          mergeRequestIId: data?.meta?.mergeRequestIId,
          mergeRequestId: data?.meta?.mergeRequestId,
        },
        page: result.nextPage,
      },
    )
  }
}

const handleStarsStream: GitlabStreamHandler = async (ctx, api, data) => {
  const result = await getStars({ api, projectId: data.projectId, page: data.page, ctx })
  await handleApiResult({
    ctx,
    result,
    activityType: GitlabActivityType.STAR,
    streamType: GitlabStreamType.STARS,
    dataId: null,
    projectId: data.projectId,
    pathWithNamespace: data.pathWithNamespace,
  })
}

const handleForksStream: GitlabStreamHandler = async (ctx, api, data) => {
  const result = await getForks({ api, projectId: data.projectId, ctx })
  await handleApiResult({
    ctx,
    result,
    activityType: GitlabActivityType.FORK,
    streamType: GitlabStreamType.FORKS,
    dataId: null,
    projectId: data.projectId,
    pathWithNamespace: data.pathWithNamespace,
  })
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const handleApiResult: GitlabProcessStreamResultHandler<GitlabActivityData<any>[]> = async ({
  ctx,
  result,
  activityType,
  streamType,
  projectId,
  pathWithNamespace,
  dataId,
  meta,
}) => {
  for (const item of result.data) {
    await ctx.processData<GitlabApiData<typeof item.data>>({
      data: {
        data: item.data,
        user: item.user,
      },
      type: activityType,
      projectId,
      pathWithNamespace,
    })
  }

  if (result.nextPage) {
    await ctx.publishStream<GitlabBasicStream>(
      `${streamType}:${projectId}${dataId ? `:${dataId}` : ''}:${result.nextPage}`,
      {
        projectId,
        pathWithNamespace,
        page: result.nextPage,
        meta,
      },
    )
  }
}

const handler: ProcessStreamHandler = async (ctx) => {
  const streamIdentifier = ctx.stream.identifier
  const data = ctx.stream.data

  const api = new Gitlab({
    oauthToken: ctx.integration.token as string,
  })

  if (streamIdentifier.startsWith(GitlabStreamType.ROOT)) {
    await handleRootStream(ctx, data as GitlabRootStream)
  } else {
    const streamType = streamIdentifier.split(':')[0] as GitlabStreamType

    switch (streamType) {
      case GitlabStreamType.ISSUES:
        await handleIssuesStream(ctx, api, data as GitlabBasicStream)
        break
      case GitlabStreamType.ISSUE_DISCUSSIONS:
        await handleIssueDiscussionsStream(ctx, api, data as GitlabBasicStream)
        break
      case GitlabStreamType.MERGE_REQUESTS:
        await handleMergeRequestsStream(ctx, api, data as GitlabBasicStream)
        break
      case GitlabStreamType.MERGE_REQUEST_DISCUSSIONS_AND_EVENTS:
        await handleMergeRequestDiscussionsAndEvents(ctx, api, data as GitlabBasicStream)
        break
      case GitlabStreamType.STARS:
        await handleStarsStream(ctx, api, data as GitlabBasicStream)
        break
      case GitlabStreamType.FORKS:
        await handleForksStream(ctx, api, data as GitlabBasicStream)
        break
      case GitlabStreamType.MERGE_REQUEST_COMMITS:
        await handleMergeRequestCommitsStream(ctx, api, data as GitlabBasicStream)
        break
      default:
        throw new Error(`Unsupported stream type: ${streamType}`)
    }
  }
}

export default handler
