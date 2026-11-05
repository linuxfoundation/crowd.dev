import {
  Error404,
  Error409,
  GithubPublicClientError,
  IGithubAccount,
  fetchCommitAuthor,
} from '@crowd/common'
import { CommonMemberService, getGithubInstallationToken } from '@crowd/common_services'
import {
  GithubEmailLookupResult,
  IGitAuthor,
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

export interface IResolveGitAuthorsPageResult {
  nextEmail: string | null
  lookups: number
  rateLimited: boolean
}

export async function resolveGitAuthorsPage(
  afterEmail: string | null,
): Promise<IResolveGitAuthorsPageResult> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const memberService = new CommonMemberService(qx, svc.temporal, svc.log)
  const { authors, nextEmail } = await findGitAuthorsPage(qx, afterEmail, 200)

  for (const [index, author] of authors.entries()) {
    try {
      await resolveGitAuthor(qx, memberService, author)
    } catch (err) {
      if (err instanceof GithubPublicClientError && err.kind === 'RATE_LIMIT') {
        svc.log.warn('GitHub rate limit reached, pausing git author lookups')
        return { nextEmail: afterEmail, lookups: index, rateLimited: true }
      }
      throw err
    }
  }

  return { nextEmail, lookups: authors.length, rateLimited: false }
}

async function resolveGitAuthor(
  qx: QueryExecutor,
  memberService: CommonMemberService,
  author: IGitAuthor,
): Promise<void> {
  const token = await getGithubInstallationToken()
  const commit = await fetchCommitAuthor(author.repoUrl, author.commitSha, token)

  if (commit.status !== 'found') {
    await recordLookup(qx, author, commit.status)
    return
  }

  const result = await linkGithubAccount(qx, memberService, author, commit.account)
  if (result) {
    await recordLookup(qx, author, result, commit.account)
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
  if (owner.githubId && owner.githubId !== account.id) {
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

  if (!identity) {
    return null
  }

  await syncMember(author.memberId)
  return 'identity-added'
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
