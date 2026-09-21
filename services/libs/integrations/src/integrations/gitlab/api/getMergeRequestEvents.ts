import { DiscussionNoteSchema, DiscussionSchema, Gitlab, OffsetPagination } from '@gitbeaker/rest'

import { IProcessStreamContext } from '../../../types'
import { GitlabApiResult, GitlabDisccusionCommentData } from '../types'
import { RedisSemaphore } from '../utils/lock'
import { handleGitlabError } from './errorHandler'
import { getUser } from './getUser'

export const getMergeRequestEvents = async ({
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
    apiCallType: 'getMergeRequestEvents',
    maxConcurrent: 1,
    cache: ctx.cache,
  })

  let pagination: OffsetPagination | undefined
  let discussions: DiscussionSchema[] = []

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
    throw handleGitlabError(error, `getMergeRequestEvents:${projectId}:${mergeRequestIId}`, ctx.log)
  } finally {
    await semaphore.release()
  }

  const notes = discussions.flatMap((discussion) => discussion.notes) as DiscussionNoteSchema[]

  const filteredNotes = notes.filter((note) => note.system === true)

  const users = []
  for (const note of filteredNotes) {
    if (note?.author?.id) {
      const user = await getUser(api, note.author.id, ctx)
      users.push(user)
    }
  }

  ctx.log.info({ filteredNotes, users }, 'merge request events')

  return {
    data: filteredNotes?.map((note, index) => ({
      data: note,
      user: users[index],
    })),
    nextPage: pagination.next,
  }
}
