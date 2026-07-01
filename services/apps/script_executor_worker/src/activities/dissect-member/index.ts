import MemberRepository from '@crowd/data-access-layer/src/old/apps/script_executor_worker/member.repo'
import MergeActionRepository from '@crowd/data-access-layer/src/old/apps/script_executor_worker/mergeAction.repo'
import { IFindMemberIdentitiesGroupedByPlatformResult } from '@crowd/data-access-layer/src/old/apps/script_executor_worker/types'
import { IMember, IMergeAction } from '@crowd/types'

import { svc } from '../../main'

export async function findMemberMergeActions(
  memberId: string,
  startDate: string,
  endDate: string,
  userId: string,
  limit: number,
): Promise<IMergeAction[]> {
  let mergeActions: IMergeAction[] = []

  try {
    const mergeActionRepo = new MergeActionRepository(svc.postgres.reader.connection(), svc.log)
    mergeActions = await mergeActionRepo.findMemberMergeActions(
      memberId,
      startDate,
      endDate,
      userId,
      limit,
    )
  } catch (err) {
    throw new Error(err)
  }

  return mergeActions
}

export async function findMergeActionUnmergeBackup(
  mergeActionId: string,
): Promise<IMergeAction['unmergeBackup'] | null> {
  try {
    const mergeActionRepo = new MergeActionRepository(svc.postgres.reader.connection(), svc.log)
    return await mergeActionRepo.findMergeActionUnmergeBackup(mergeActionId)
  } catch (err) {
    throw new Error(err)
  }
}

export async function findMemberIdentitiesGroupedByPlatform(
  memberId: string,
): Promise<IFindMemberIdentitiesGroupedByPlatformResult[]> {
  let groupedIdentities: IFindMemberIdentitiesGroupedByPlatformResult[] = []

  try {
    const memberRepo = new MemberRepository(svc.postgres.reader.connection(), svc.log)
    groupedIdentities = await memberRepo.findMemberIdentitiesGroupedByPlatform(memberId)
  } catch (err) {
    throw new Error(err)
  }

  return groupedIdentities
}

export async function findMemberById(memberId: string): Promise<IMember | null> {
  let member: IMember

  try {
    const memberRepo = new MemberRepository(svc.postgres.reader.connection(), svc.log)
    member = await memberRepo.findMemberById(memberId)
  } catch (err) {
    throw new Error(err)
  }

  return member
}
