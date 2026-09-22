import { Gitlab, UserSchema } from '@gitbeaker/rest'

import { timeout } from '@crowd/common'

import {
  IProcessStreamContext,
  IProcessWebhookStreamContext,
  ProcessWebhookStreamHandler,
} from '../../types'
import { getUser } from './api/getUser'
import {
  handleMergeRequestCommitsStream,
  handleMergeRequestDiscussionsAndEvents,
} from './processStream'
import {
  GitlabApiData,
  GitlabBasicStream,
  GitlabIssueCommentWebhook,
  GitlabIssueWebhook,
  GitlabMergeRequestCommentWebhook,
  GitlabMergeRequestWebhook,
  GitlabStreamType,
  GitlabWebhook,
  GitlabWebhookType,
} from './types'
import verifyGitlabWebhook from './utils/verifyWebhook'

interface GitlabWebhookHandler {
  (
    ctx: IProcessWebhookStreamContext,
    api: InstanceType<typeof Gitlab>,
    payload: GitlabWebhook,
  ): Promise<void>
}

const handleIssueWebhook: GitlabWebhookHandler = async (ctx, api, payload) => {
  const data = payload.data as GitlabIssueWebhook

  let user: UserSchema

  if (data.object_attributes.action === 'open' || data.object_attributes.action === 'update') {
    user = await getUser(api, data.object_attributes.author_id, ctx as IProcessStreamContext)
    await ctx.processData<GitlabApiData<typeof data>>({
      data: {
        data: data,
        user: user,
      },
      type: GitlabWebhookType.ISSUE_OPENED_OR_UPDATED,
      projectId: data.project.id.toString(),
      pathWithNamespace: data.project.path_with_namespace,
      isWebhook: true,
    })
  } else if (data.object_attributes.action === 'close') {
    user = await getUser(api, data.user.id, ctx as IProcessStreamContext)
    await ctx.processData<GitlabApiData<typeof data>>({
      data: {
        data: data,
        user: user,
      },
      type: GitlabWebhookType.ISSUE_CLOSED,
      projectId: data.project.id.toString(),
      pathWithNamespace: data.project.path_with_namespace,
      isWebhook: true,
    })
  } else {
    return
  }
}

const handleIssueCommentWebhook: GitlabWebhookHandler = async (ctx, api, payload) => {
  const data = payload.data as GitlabIssueCommentWebhook

  if (data.object_attributes.action === 'create' && data.object_attributes.system === false) {
    const user = await getUser(api, data.object_attributes.author_id, ctx as IProcessStreamContext)

    await ctx.processData<GitlabApiData<typeof data>>({
      data: {
        data: data,
        user: user,
      },
      type: GitlabWebhookType.ISSUE_COMMENT,
      projectId: data.project.id.toString(),
      pathWithNamespace: data.project.path_with_namespace,
      isWebhook: true,
    })
  }
}

const handleMergeRequestWebhook: GitlabWebhookHandler = async (ctx, api, payload) => {
  const data = payload.data as GitlabMergeRequestWebhook

  let user: UserSchema

  if (data.object_attributes.action === 'open') {
    user = await getUser(api, data.object_attributes.author_id, ctx as IProcessStreamContext)
    await ctx.processData<GitlabApiData<typeof data>>({
      data: {
        data: data,
        user: user,
      },
      type: GitlabWebhookType.MERGE_REQUEST_OPENED,
      projectId: data.project.id.toString(),
      pathWithNamespace: data.project.path_with_namespace,
      isWebhook: true,
    })
  } else if (data.object_attributes.action === 'close') {
    user = await getUser(api, data.user.id, ctx as IProcessStreamContext)
    await ctx.processData<GitlabApiData<typeof data>>({
      data: {
        data: data,
        user: user,
      },
      type: GitlabWebhookType.MERGE_REQUEST_CLOSED,
      projectId: data.project.id.toString(),
      pathWithNamespace: data.project.path_with_namespace,
      isWebhook: true,
    })
  } else if (data.object_attributes.action === 'merge') {
    user = await getUser(api, data.user.id, ctx as IProcessStreamContext)
    await ctx.processData<GitlabApiData<typeof data>>({
      data: {
        data: data,
        user: user,
      },
      type: GitlabWebhookType.MERGE_REQUEST_MERGED,
      projectId: data.project.id.toString(),
      pathWithNamespace: data.project.path_with_namespace,
      isWebhook: true,
    })
  } else if (data.object_attributes.action === 'approved') {
    // do nothing - go on to trigger merge request events and commits
  } else if (data.object_attributes.action === 'update') {
    // do nothing - go on to trigger merge request events and commits
  } else {
    ctx.log.warn('Unsupported Gitlab webhook type', data.object_attributes.action)
    return
  }

  // get merge request commits
  await ctx.publishStream<GitlabBasicStream>(
    `${GitlabStreamType.MERGE_REQUEST_COMMITS}:${data.project.id}:${data.object_attributes.id}:firstPage`,
    {
      projectId: data.project.id.toString(),
      pathWithNamespace: data.project.path_with_namespace,
      meta: {
        mergeRequestIId: data.object_attributes.iid,
        mergeRequestId: data.object_attributes.id,
      },
      page: 1,
    },
  )

  await timeout(3000)

  // get merge request events and discussions
  await ctx.publishStream<GitlabBasicStream>(
    `${GitlabStreamType.MERGE_REQUEST_DISCUSSIONS_AND_EVENTS}:${data.project.id}:${data.object_attributes.id}:firstPage`,
    {
      projectId: data.project.id.toString(),
      pathWithNamespace: data.project.path_with_namespace,
      meta: {
        mergeRequestIId: data.object_attributes.iid,
        mergeRequestId: data.object_attributes.id,
      },
      page: 1,
    },
  )
}

const handleMergeRequestCommentWebhook: GitlabWebhookHandler = async (ctx, api, payload) => {
  const data = payload.data as GitlabMergeRequestCommentWebhook

  if (data.object_attributes.action === 'create' && data.object_attributes.system === false) {
    const user = await getUser(api, data.object_attributes.author_id, ctx as IProcessStreamContext)

    await ctx.processData<GitlabApiData<typeof data>>({
      data: {
        data: data,
        user: user,
      },
      type: GitlabWebhookType.MERGE_REQUEST_COMMENT,
      projectId: data.project.id.toString(),
      pathWithNamespace: data.project.path_with_namespace,
      isWebhook: true,
    })
  }
}

const handler: ProcessWebhookStreamHandler = async (ctx) => {
  const identifier = ctx.stream.identifier

  const api = new Gitlab({
    oauthToken: ctx.integration.token as string,
  })

  if (identifier.startsWith(GitlabStreamType.MERGE_REQUEST_COMMITS)) {
    await handleMergeRequestCommitsStream(
      ctx as IProcessStreamContext,
      api,
      ctx.stream.data as GitlabBasicStream,
    )
    return
  } else if (identifier.startsWith(GitlabStreamType.MERGE_REQUEST_DISCUSSIONS_AND_EVENTS)) {
    await handleMergeRequestDiscussionsAndEvents(
      ctx as IProcessStreamContext,
      api,
      ctx.stream.data as GitlabBasicStream,
    )
    return
  } else {
    // this is for normal webhook events
    const payload = ctx.stream.data as GitlabWebhook
    const { data, headers } = payload

    try {
      verifyGitlabWebhook(ctx, headers)
    } catch (error) {
      ctx.log.error(error, 'Failed to verify Gitlab webhook', data, headers)
      throw error
    }

    let type: 'note' | 'issue' | 'issue_comment' | 'merge_request_comment' | 'merge_request' =
      data.object_kind

    if (type === 'note' && 'issue' in data) {
      type = 'issue_comment'
    } else if (type === 'note' && 'merge_request' in data) {
      type = 'merge_request_comment'
    }

    switch (type) {
      case 'issue':
        await handleIssueWebhook(ctx, api, payload)
        break
      case 'issue_comment':
        await handleIssueCommentWebhook(ctx, api, payload)
        break
      case 'merge_request':
        await handleMergeRequestWebhook(ctx, api, payload)
        break
      case 'merge_request_comment':
        await handleMergeRequestCommentWebhook(ctx, api, payload)
        break
      default:
        throw new Error(`Unsupported Gitlab webhook type: ${type}`)
    }
  }
}

export default handler
