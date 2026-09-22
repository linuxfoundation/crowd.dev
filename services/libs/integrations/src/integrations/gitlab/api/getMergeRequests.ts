import { Gitlab, MergeRequestSchema, OffsetPagination } from '@gitbeaker/rest'

import { IProcessStreamContext } from '../../../types'
import { GitlabApiResult, GitlabMergeRequestData } from '../types'
import { RedisSemaphore } from '../utils/lock'
import { handleGitlabError } from './errorHandler'
import { getUser } from './getUser'

export const getMergeRequests = async ({
  api,
  projectId,
  page,
  ctx,
}: {
  api: InstanceType<typeof Gitlab>
  projectId: string
  page: number
  ctx: IProcessStreamContext
}): Promise<GitlabApiResult<GitlabMergeRequestData[]>> => {
  const since = ctx.onboarding
    ? undefined
    : new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString()
  const perPage = 20

  const semaphore = new RedisSemaphore({
    integrationId: ctx.integration.id,
    apiCallType: 'getMergeRequests',
    maxConcurrent: 1,
    cache: ctx.cache,
  })

  let pagination: OffsetPagination | undefined
  let mergeRequests: MergeRequestSchema[] = []

  try {
    await semaphore.acquire()
    const response = await api.MergeRequests.all({
      projectId,
      page,
      perPage,
      updatedAfter: since,
      showExpanded: true,
    })

    mergeRequests = response.data as MergeRequestSchema[]
    pagination = response.paginationInfo
  } catch (error) {
    if (error.message === '403 Forbidden' || error.message === '404 Project Not Found') {
      return {
        data: [],
        nextPage: null,
      }
    }
    throw handleGitlabError(error, `getMergeRequests:${projectId}`, ctx.log)
  } finally {
    await semaphore.release()
  }

  const users = []
  for (const mr of mergeRequests) {
    const user = await getUser(api, mr.author.id, ctx)
    users.push(user)
  }

  return {
    data: mergeRequests.map((mr, index) => ({
      data: mr,
      user: users[index],
    })),
    nextPage: pagination.next,
  }
}
