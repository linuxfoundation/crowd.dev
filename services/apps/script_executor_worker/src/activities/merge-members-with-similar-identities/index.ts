import MemberRepository from '@crowd/data-access-layer/src/old/apps/script_executor_worker/member.repo'
import { ISimilarMember } from '@crowd/data-access-layer/src/old/apps/script_executor_worker/types'

import { svc } from '../../main'

export async function findMembersWithSameVerifiedEmailsInDifferentPlatforms(
  limit: number,
  afterHighMemberId?: string,
  afterLowMemberId?: string,
): Promise<ISimilarMember[]> {
  let rows: ISimilarMember[] = []

  try {
    const memberRepo = new MemberRepository(svc.postgres.reader.connection(), svc.log)
    rows = await memberRepo.findMembersWithSameVerifiedEmailsInDifferentPlatforms(
      limit,
      afterHighMemberId,
      afterLowMemberId,
    )
  } catch (err) {
    throw new Error(err)
  }

  return rows
}

export async function findMembersWithSamePlatformIdentitiesDifferentCapitalization(
  platform: string,
  limit: number,
  afterHash?: number,
): Promise<ISimilarMember[]> {
  let rows: ISimilarMember[] = []

  try {
    const memberRepo = new MemberRepository(svc.postgres.reader.connection(), svc.log)
    rows = await memberRepo.findMembersWithSameGithubIdentitiesDifferentCapitalization(
      platform,
      limit,
      afterHash,
    )
  } catch (err) {
    throw new Error(err)
  }

  return rows
}
