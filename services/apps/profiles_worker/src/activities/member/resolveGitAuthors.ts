import { ApplicationFailure } from '@temporalio/client'

import {
  Error404,
  Error409,
  GithubCommitAuthor,
  GithubPublicClientError,
  IGithubAccount,
  fetchCommitAuthor,
} from '@crowd/common'
import { CommonMemberService, getGithubInstallationToken } from '@crowd/common_services'
import {
  GithubEmailLookupResult,
  IGitAuthor,
  IGitAuthorsPage,
  QueryExecutor,
  findGitAuthorsPage,
  findGithubAccountOwner,
  getMemberNoMerge,
  insertGithubEmailLookup,
  insertMemberIdentities,
  pgpQx,
} from '@crowd/data-access-layer'
import { MemberIdentityType, PlatformType } from '@crowd/types'

import { svc } from '../../main'
import { syncMember } from './memberUpdate'

const ONE_MINUTE_MS = 60 * 1000
const ONE_HOUR_MS = 60 * ONE_MINUTE_MS

export async function getGitAuthorsPage(afterEmail: string | null): Promise<IGitAuthorsPage> {
  return findGitAuthorsPage(pgpQx(svc.postgres.writer.connection()), afterEmail, 500)
}

export async function resolveGitAuthor(author: IGitAuthor): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const token = await getGithubInstallationToken()

  let commit: GithubCommitAuthor
  try {
    commit = await fetchCommitAuthor(author.repoUrl, author.commitSha, token)
  } catch (err) {
    if (err instanceof GithubPublicClientError && err.kind === 'RATE_LIMIT') {
      const resetInMs = (err.resetAtMs ?? 0) - Date.now()
      throw ApplicationFailure.create({
        message: err.message,
        nextRetryDelay: Math.min(Math.max(resetInMs, ONE_MINUTE_MS), ONE_HOUR_MS),
      })
    }
    throw err
  }

  if (commit.status !== 'found') {
    await recordLookup(qx, author, commit.status)
    return
  }

  const memberService = new CommonMemberService(qx, svc.temporal, svc.log)
  const result = await linkGithubAccount(qx, memberService, author, commit.account)
  if (!result) {
    return
  }

  await recordLookup(qx, author, result, commit.account)
  if (result === 'identity-added') {
    await syncMember(author.memberId)
  }
}

async function linkGithubAccount(
  qx: QueryExecutor,
  memberService: CommonMemberService,
  author: IGitAuthor,
  account: IGithubAccount,
): Promise<GithubEmailLookupResult | null> {
  const owner = await findGithubAccountOwner(qx, account.login)

  if (!owner) {
    return addGithubIdentity(qx, author, account)
  }
  // A retried attempt finds the identity it already added.
  if (owner.memberId === author.memberId) {
    return 'identity-added'
  }
  if (owner.githubId && ![account.id, account.nodeId].includes(owner.githubId)) {
    return 'github-id-mismatch'
  }
  if (await isNoMergePair(qx, author.memberId, owner.memberId)) {
    return 'no-merge'
  }

  return mergeIntoOwner(memberService, owner.memberId, author.memberId)
}

async function addGithubIdentity(
  qx: QueryExecutor,
  author: IGitAuthor,
  account: IGithubAccount,
): Promise<GithubEmailLookupResult | null> {
  const [identity] = await insertMemberIdentities(
    qx,
    [
      {
        memberId: author.memberId,
        integrationId: author.integrationId,
        platform: PlatformType.GITHUB,
        value: account.login,
        type: MemberIdentityType.USERNAME,
        verified: true,
        source: 'integration',
        sourceId: account.id,
      },
    ],
    false,
    true,
  )

  return identity ? 'identity-added' : null
}

async function isNoMergePair(
  qx: QueryExecutor,
  memberId: string,
  ownerId: string,
): Promise<boolean> {
  const pairs = await getMemberNoMerge(qx, [memberId], { includeExpired: true })
  return pairs.some((pair) => pair.memberId === ownerId || pair.noMergeId === ownerId)
}

async function mergeIntoOwner(
  memberService: CommonMemberService,
  ownerId: string,
  memberId: string,
): Promise<GithubEmailLookupResult | null> {
  try {
    await memberService.merge(ownerId, memberId)
    return 'merged'
  } catch (err) {
    if (err instanceof Error404 || err instanceof Error409) {
      svc.log.info({ ownerId, memberId }, 'Skipping merge, a member is gone or already merging')
      return null
    }
    throw err
  }
}

async function recordLookup(
  qx: QueryExecutor,
  author: IGitAuthor,
  result: GithubEmailLookupResult,
  account?: IGithubAccount,
): Promise<void> {
  await insertGithubEmailLookup(qx, {
    email: author.email,
    result,
    githubId: account?.id ?? null,
    githubLogin: account?.login ?? null,
    commitSha: author.commitSha,
  })
}
