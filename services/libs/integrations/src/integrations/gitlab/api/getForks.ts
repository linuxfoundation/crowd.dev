import { Gitlab, ProjectSchema } from '@gitbeaker/rest'

import { IProcessStreamContext } from '../../../types'
import { GitlabForkData } from '../types'
import { GitlabApiResult } from '../types'
import { RedisSemaphore } from '../utils/lock'
import { handleGitlabError } from './errorHandler'
import { getUser } from './getUser'

export const getForks = async ({
  api,
  projectId,
  ctx,
}: {
  api: InstanceType<typeof Gitlab>
  projectId: string
  ctx: IProcessStreamContext
}): Promise<GitlabApiResult<GitlabForkData[]>> => {
  const since = ctx.onboarding
    ? undefined
    : new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString()

  const semaphore = new RedisSemaphore({
    integrationId: ctx.integration.id,
    apiCallType: 'getForks',
    maxConcurrent: 1,
    cache: ctx.cache,
  })

  let forks: ProjectSchema[] = []

  try {
    await semaphore.acquire()
    forks = (await api.Projects.allForks(projectId, {
      updatedAfter: since,
    })) as ProjectSchema[]
  } catch (error) {
    if (error.message === '404 Project Not Found') {
      return {
        data: [],
        nextPage: null,
      }
    }
    throw handleGitlabError(error, `getForks:${projectId}`, ctx.log)
  } finally {
    await semaphore.release()
  }

  const users = []
  for (const fork of forks) {
    const user = await getUser(api, fork.owner?.id || fork.creator_id, ctx)
    users.push(user)
  }

  ctx.log.info({ forks, users }, 'forks')

  return {
    data: forks.map((fork, index) => ({
      data: fork,
      user: users[index],
    })),
    nextPage: null,
  }
}
