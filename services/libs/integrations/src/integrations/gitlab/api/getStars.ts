import { Gitlab, ProjectStarrerSchema } from '@gitbeaker/rest'

import { IProcessStreamContext } from '../../../types'
import { GitlabApiResult, GitlabStarData } from '../types'
import { RedisSemaphore } from '../utils/lock'
import { handleGitlabError } from './errorHandler'
import { getUser } from './getUser'

export const getStars = async ({
  api,
  projectId,
  ctx,
}: {
  api: InstanceType<typeof Gitlab>
  projectId: string
  page: number
  ctx: IProcessStreamContext
}): Promise<GitlabApiResult<GitlabStarData[]>> => {
  const semaphore = new RedisSemaphore({
    integrationId: ctx.integration.id,
    apiCallType: 'getStars',
    maxConcurrent: 1,
    cache: ctx.cache,
  })

  let stars: ProjectStarrerSchema[] = []

  try {
    await semaphore.acquire()
    stars = (await api.Projects.allStarrers(projectId)) as ProjectStarrerSchema[]
  } catch (error) {
    if (error.message === '404 Project Not Found') {
      return {
        data: [],
        nextPage: null,
      }
    }
    throw handleGitlabError(error, `getStars:${projectId}`, ctx.log)
  } finally {
    await semaphore.release()
  }

  const users = []
  for (const star of stars) {
    const user = await getUser(api, star.user.id, ctx)
    users.push(user)
  }

  ctx.log.info({ stars, users }, 'stars')

  return {
    data: stars.map((star, index) => ({
      data: star,
      user: users[index],
    })),
    nextPage: null,
  }
}
