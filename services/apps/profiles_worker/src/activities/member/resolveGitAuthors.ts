import { ApplicationFailure } from '@temporalio/client'

import {
  Error404,
  Error409,
  GithubCommitAuthor,
  GithubPublicClientError,
  IGithubAccount,
  fetchCommitAuthor,
  parseGithubUrl,
} from '@crowd/common'
import { CommonMemberService, getGithubInstallationToken } from '@crowd/common_services'
import {
  GithubEmailLookupResult,
  IGitAuthor,
  IGitAuthorsPage,
  QueryExecutor,
  findGitAuthorCommitOutsideOwners,
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
const MAX_REPO_OWNERS_TRIED = 3

export type GitAuthorResolution =
  | { status: 'resolved' }
  | { status: 'rate-limited'; waitMs: number }

interface ICommitAuthorLookup {
  author: IGitAuthor
  commit: GithubCommitAuthor
}

export async function getGitAuthorsPage(afterEmail: string | null): Promise<IGitAuthorsPage> {
  return findGitAuthorsPage(pgpQx(svc.postgres.writer.connection()), afterEmail, 500)
}

export async function resolveGitAuthor(gitAuthor: IGitAuthor): Promise<GitAuthorResolution> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const token = await getGithubInstallationToken()

  let lookup: ICommitAuthorLookup
  try {
    lookup = await lookUpReachableCommitAuthor(qx, gitAuthor, token)
  } catch (err) {
    if (err instanceof GithubPublicClientError && err.kind === 'RATE_LIMIT') {
      const resetInMs = (err.resetAtMs ?? 0) - Date.now()
      return {
        status: 'rate-limited',
        waitMs: Math.min(Math.max(resetInMs, ONE_MINUTE_MS), ONE_HOUR_MS),
      }
    }
    if (err instanceof GithubPublicClientError && err.kind !== 'TRANSIENT') {
      throw ApplicationFailure.nonRetryable(err.message, err.kind)
    }
    throw err
  }

  const { author, commit } = lookup
  if (commit.status !== 'found') {
    const account = commit.status === 'bot-account' ? commit.account : undefined
    await recordLookup(qx, author, commit.status, account)
    return { status: 'resolved' }
  }

  const memberService = new CommonMemberService(qx, svc.temporal, svc.log)
  const result = await linkGithubAccount(qx, memberService, author, commit.account)
  if (!result) {
    return { status: 'resolved' }
  }

  await recordLookup(qx, author, result, commit.account)
  if (result === 'identity-added') {
    await syncMember(author.memberId)
  }
  return { status: 'resolved' }
}

async function lookUpReachableCommitAuthor(
  qx: QueryExecutor,
  author: IGitAuthor,
  token: string,
): Promise<ICommitAuthorLookup> {
  const triedOwners: string[] = []
  let candidate: IGitAuthor | null = author

  while (candidate) {
    const commit = await fetchCommitAuthor(candidate.repoUrl, candidate.commitSha, token)
    if (commit.status !== 'repo-inaccessible') {
      return { author: candidate, commit }
    }

    triedOwners.push(parseGithubUrl(candidate.repoUrl).owner)
    if (triedOwners.length === MAX_REPO_OWNERS_TRIED) {
      break
    }
    const nextCommit = await findGitAuthorCommitOutsideOwners(qx, author, triedOwners)
    candidate = nextCommit && { ...author, ...nextCommit }
  }

  return { author, commit: { status: 'repo-inaccessible' } }
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
  if (owner.isBot) {
    return 'bot-account'
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
