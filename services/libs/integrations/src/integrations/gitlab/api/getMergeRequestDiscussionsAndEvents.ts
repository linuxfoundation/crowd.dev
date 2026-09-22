import { DiscussionNoteSchema, DiscussionSchema, Gitlab, OffsetPagination } from '@gitbeaker/rest'

import { IProcessStreamContext } from '../../../types'
import { GitlabApiResult, GitlabDisccusionCommentData } from '../types'
import { RedisSemaphore } from '../utils/lock'
import { handleGitlabError } from './errorHandler'
import { getUser } from './getUser'

export const getMergeRequestDiscussionsAndEvents = async ({
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
}): Promise<GitlabApiResult<GitlabDisccusionCommentData[]>> => {
  const perPage = 20

  const semaphore = new RedisSemaphore({
    integrationId: ctx.integration.id,
    apiCallType: 'getMergeRequestDiscussionsAndEvents',
    maxConcurrent: 1,
    cache: ctx.cache,
  })

  let pagination: OffsetPagination
  let discussions: DiscussionSchema[]

  try {
    await semaphore.acquire()
    const response = await api.MergeRequestDiscussions.all(projectId, mergeRequestIId, {
      showExpanded: true,
      page,
      perPage,
    })

    discussions = response.data as DiscussionSchema[]
    pagination = response.paginationInfo
  } catch (error) {
    throw handleGitlabError(
      error,
      `getMergeRequestDiscussionsAndEvents:${projectId}:${mergeRequestIId}`,
      ctx.log,
    )
  } finally {
    await semaphore.release()
  }

  const notes = discussions.flatMap((discussion) => discussion.notes) as DiscussionNoteSchema[]

  const users = []
  for (const note of notes) {
    if (note?.author?.id) {
      const user = await getUser(api, note.author.id, ctx)
      users.push(user)
    }
  }

  ctx.log.info({ notes, users }, 'merge request discussions and events')

  return {
    data: notes.map((note, index) => ({
      data: note,
      user: users[index],
    })),
    nextPage: pagination.next,
  }
}
