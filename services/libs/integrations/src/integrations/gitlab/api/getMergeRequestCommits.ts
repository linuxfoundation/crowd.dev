import { CommitSchema, ExpandedCommitSchema, Gitlab, OffsetPagination } from '@gitbeaker/rest'

import { timeout } from '@crowd/common'

import { IProcessStreamContext } from '../../../types'
import { GitlabApiResult, GitlabMergeRequestCommitData } from '../types'
import { RedisSemaphore } from '../utils/lock'
import { handleGitlabError } from './errorHandler'

export const getMergeRequestCommits = async ({
  api,
  projectId,
  mergeRequestIId,
  page,
  ctx,
}: {
  api: InstanceType<typeof Gitlab>
  projectId: string
  mergeRequestIId: number
  page: number
  ctx: IProcessStreamContext
}): Promise<GitlabApiResult<GitlabMergeRequestCommitData[]>> => {
  const semaphore = new RedisSemaphore({
    integrationId: ctx.integration.id,
    apiCallType: 'getMergeRequestCommits',
    maxConcurrent: 1,
    cache: ctx.cache,
    timeout: 120000,
  })

  let pagination: OffsetPagination
  const extendedCommits: ExpandedCommitSchema[] = []

  try {
    await semaphore.acquire()
    const response = await api.MergeRequests.allCommits(projectId, mergeRequestIId, {
      page,
      perPage: 20,
      showExpanded: true,
    })

    const commits = response.data as CommitSchema[]

    for (const commit of commits) {
      try {
        const extendedCommit = await api.Commits.show(projectId, commit.id, {
          stats: true,
        })
        extendedCommits.push(extendedCommit as ExpandedCommitSchema)
        await timeout(500)
      } catch (error) {
        const handledError = handleGitlabError(
          error,
          `getMergeRequestCommits:getCommit:${projectId}:${commit.id}`,
          ctx.log,
        )
        // Only rethrow if it's a rate limit error
        if (handledError.name === 'RateLimitError') {
          throw handledError
        }
        ctx.log.error(`Failed to fetch extended commit for ${commit.id}: ${error}`)
      }
    }

    pagination = response.paginationInfo
  } catch (error) {
    throw handleGitlabError(
      error,
      `getMergeRequestCommits:${projectId}:${mergeRequestIId}`,
      ctx.log,
    )
  } finally {
    await semaphore.release()
  }

  ctx.log.info({ extendedCommits }, 'extended commits')

  return {
    data: extendedCommits.map((commit) => ({
      data: commit,
      user: null,
    })),
    nextPage: pagination.next,
  }
}
