import { DiscussionNoteSchema, DiscussionSchema, Gitlab, OffsetPagination } from '@gitbeaker/rest'

import { IProcessStreamContext } from '../../../types'
import { GitlabApiResult, GitlabDisccusionCommentData } from '../types'
import { RedisSemaphore } from '../utils/lock'
import { handleGitlabError } from './errorHandler'
import { getUser } from './getUser'

export const getIssueDiscussions = async ({
  api,
  projectId,
  issueId,
  page,
  ctx,
}: {
  api: InstanceType<typeof Gitlab>
  projectId: string
  issueId: number
  page: number
  ctx: IProcessStreamContext
}): Promise<GitlabApiResult<GitlabDisccusionCommentData[]>> => {
  const perPage = 20

  // Validate issueId before making the API call
  if (!issueId || typeof issueId !== 'number' || issueId <= 0) {
    ctx.log.error(
      { projectId, issueId, issueIdType: typeof issueId },
      'Invalid issueId provided to getIssueDiscussions',
    )
    throw new Error(
      `Invalid issueId: ${issueId} (type: ${typeof issueId}) for project ${projectId}`,
    )
  }

  const semaphore = new RedisSemaphore({
    integrationId: ctx.integration.id,
    apiCallType: 'getIssueDiscussions',
    maxConcurrent: 1,
    cache: ctx.cache,
  })

  let pagination: OffsetPagination
  let discussions: DiscussionSchema[] = []
  try {
    await semaphore.acquire()
    const response = await api.IssueDiscussions.all(projectId, issueId, {
      showExpanded: true,
      page,
      perPage,
    })

    discussions = response.data as DiscussionSchema[]
    pagination = response.paginationInfo
  } catch (error) {
    throw handleGitlabError(error, `getIssueDiscussions:${projectId}:${issueId}`, ctx.log)
  } finally {
    await semaphore.release()
  }

  const notes = discussions.flatMap((discussion) => discussion.notes) as DiscussionNoteSchema[]

  const filteredNotes = notes.filter((note) => note.system === false)

  const users = []
  for (const note of filteredNotes) {
    if (note?.author?.id) {
      const user = await getUser(api, note.author.id, ctx)
      users.push(user)
    }
  }

  ctx.log.info({ filteredNotes, users }, 'issue discussions')

  return {
    data: filteredNotes?.map((note, index) => ({
      data: note,
      user: users[index],
    })),
    nextPage: pagination.next,
  }
}
