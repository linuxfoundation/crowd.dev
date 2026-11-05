import { getErrorMessage } from '@crowd/common'
import {
  IProjectCatalogOpenCounts,
  countProjectCatalogOpenBySourceUrl,
} from '@crowd/data-access-layer'
import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import { setSlackRequestReactions } from '@crowd/slack'

import { BotStatus, STATUS_REACTION } from './botCopy'
import { organizationOf } from './organizationGroup'

const log = getServiceLogger()

const FINAL_STATUSES: ReadonlySet<BotStatus> = new Set([
  'added',
  'alreadyThere',
  'notAdded',
  'problem',
])

type RequestRef = Pick<IDbProjectCatalog, 'id' | 'repoUrl' | 'sourceUrl' | 'requestHints'>

export function reactionsToRemove(
  status: BotStatus,
  open: IProjectCatalogOpenCounts,
  organizationRequest = false,
): string[] {
  const remove: string[] = []
  if (status === 'added' && organizationRequest) {
    remove.push(STATUS_REACTION.notAdded)
  }
  if (status !== 'checking' && open.pending === 0) {
    remove.push(STATUS_REACTION.checking)
  }
  if (
    (FINAL_STATUSES.has(status) || status === 'checking' || status === 'needsInput') &&
    open.inReview === 0
  ) {
    remove.push(STATUS_REACTION.withTeam)
  }
  if (status === 'checking' && open.failed === 0) {
    remove.push(STATUS_REACTION.problem)
  }
  if (status !== 'needsInput' && open.asked === 0) {
    remove.push(STATUS_REACTION.needsInput)
  }
  return remove
}

export async function applyRequestStatus(
  qx: QueryExecutor,
  project: RequestRef,
  status: BotStatus,
): Promise<void> {
  if (!project.sourceUrl) {
    return
  }

  try {
    const open = await countProjectCatalogOpenBySourceUrl(qx, project.sourceUrl)
    const result = await setSlackRequestReactions(project.sourceUrl, {
      add: [STATUS_REACTION[status]],
      remove: reactionsToRemove(status, open, Boolean(organizationOf(project))),
    })
    if (!result.ok) {
      log.warn(
        { id: project.id, repoUrl: project.repoUrl, status, error: result.error },
        'Slack request reaction was not updated.',
      )
    }
  } catch (err) {
    log.warn(
      { id: project.id, repoUrl: project.repoUrl, status, error: getErrorMessage(err) },
      'Slack request reaction update failed.',
    )
  }
}
