import { MemberIdentityType, PlatformType } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'
import {
  IGitAuthor,
  IGitAuthorCommit,
  IGitAuthorsPage,
  IGithubAccountOwner,
  IGithubEmailLookup,
} from './types'

export * from './types'

interface IGitIdentity {
  memberId: string
  email: string
  integrationId: string | null
}

async function findGitIdentities(
  qx: QueryExecutor,
  afterEmail: string | null,
  limit: number,
): Promise<IGitIdentity[]> {
  return qx.select(
    `
      select "memberId", lower(value) as email, "integrationId"
      from "memberIdentities"
      where platform = $(platform)
        and type = $(type)
        and verified = true
        and "deletedAt" is null
        and lower(value) > $(afterEmail)
      order by lower(value)
      limit $(limit)
    `,
    {
      platform: PlatformType.GIT,
      type: MemberIdentityType.USERNAME,
      afterEmail: afterEmail ?? '',
      limit,
    },
  )
}

async function findUnresolvedGitAuthors(
  qx: QueryExecutor,
  identities: IGitIdentity[],
): Promise<IGitAuthor[]> {
  return qx.select(
    `
      -- one email per member: resolving it can merge the member away, so the rest wait a pass
      select distinct on (i."memberId")
        i."memberId", i.email, i."integrationId", c."repoUrl", c."commitSha"
      from unnest($(memberIds)::uuid[], $(emails)::text[], $(integrationIds)::uuid[])
        as i("memberId", email, "integrationId")
      join members m on m.id = i."memberId"
        and m."deletedAt" is null
        and coalesce((m.attributes -> 'isBot' ->> 'default')::boolean, false) = false
      cross join lateral (
        select ar.channel as "repoUrl", ar."sourceId" as "commitSha"
        from "activityRelations" ar
        where ar."memberId" = i."memberId"
          and ar.platform = $(platform)
          and ar.type = 'authored-commit'
          and ar.channel like 'https://github.com/%'
          and lower(ar.username) = i.email
        limit 1
      ) c
      where i.email not like '%@users.noreply.github.com'
        and not exists (select 1 from "githubEmailLookups" l where l.email = i.email)
        and not exists (
          select 1
          from "memberIdentities" other
          where other."memberId" = i."memberId"
            and other."deletedAt" is null
            and (other.platform <> $(platform) or lower(other.value) like '%@users.noreply.github.com')
        )
      order by i."memberId", i.email
    `,
    {
      platform: PlatformType.GIT,
      memberIds: identities.map((identity) => identity.memberId),
      emails: identities.map((identity) => identity.email),
      integrationIds: identities.map((identity) => identity.integrationId),
    },
  )
}

export async function findGitAuthorsPage(
  qx: QueryExecutor,
  afterEmail: string | null,
  limit: number,
): Promise<IGitAuthorsPage> {
  const identities = await findGitIdentities(qx, afterEmail, limit)
  if (identities.length === 0) {
    return { authors: [], nextEmail: null }
  }

  return {
    authors: await findUnresolvedGitAuthors(qx, identities),
    nextEmail: identities.length < limit ? null : identities[identities.length - 1].email,
  }
}

export async function findGitAuthorCommitOutsideOwners(
  qx: QueryExecutor,
  author: Pick<IGitAuthor, 'memberId' | 'email'>,
  excludedOwners: string[],
): Promise<IGitAuthorCommit | null> {
  return qx.selectOneOrNone(
    `
      select ar.channel as "repoUrl", ar."sourceId" as "commitSha"
      from "activityRelations" ar
      where ar."memberId" = $(memberId)
        and ar.platform = $(platform)
        and ar.type = 'authored-commit'
        and ar.channel like 'https://github.com/%'
        and lower(ar.username) = $(email)
        and lower(split_part(ar.channel, '/', 4)) <> all($(excludedOwners)::text[])
      limit 1
    `,
    {
      memberId: author.memberId,
      email: author.email,
      platform: PlatformType.GIT,
      excludedOwners,
    },
  )
}

export async function findGithubAccountOwner(
  qx: QueryExecutor,
  login: string,
): Promise<IGithubAccountOwner | null> {
  return qx.selectOneOrNone(
    `
      select
        mi."memberId",
        mi."sourceId" as "githubId",
        coalesce((m.attributes -> 'isBot' ->> 'default')::boolean, false) as "isBot"
      from "memberIdentities" mi
      join members m on m.id = mi."memberId"
      where mi.platform = $(platform)
        and mi.type = $(type)
        and mi.verified = true
        and mi."deletedAt" is null
        and lower(mi.value) = lower($(login))
    `,
    { platform: PlatformType.GITHUB, type: MemberIdentityType.USERNAME, login },
  )
}

export async function insertGithubEmailLookup(
  qx: QueryExecutor,
  lookup: IGithubEmailLookup,
): Promise<void> {
  await qx.result(
    `
      insert into "githubEmailLookups" (email, result, "githubId", "githubLogin", "commitSha")
      values ($(email), $(result), $(githubId), $(githubLogin), $(commitSha))
      on conflict (email) do nothing
    `,
    lookup,
  )
}

export async function countGithubEmailLookupsByMember(
  qx: QueryExecutor,
  memberId: string,
): Promise<number> {
  const { count } = await qx.selectOne(
    `
      select count(*)::int as count
      from "githubEmailLookups"
      where email in (select lower(value) from "memberIdentities" where "memberId" = $(memberId))
    `,
    { memberId },
  )
  return count
}

export async function deleteGithubEmailLookupsByMember(
  qx: QueryExecutor,
  memberId: string,
): Promise<number> {
  return qx.result(
    `
      delete from "githubEmailLookups"
      where email in (select lower(value) from "memberIdentities" where "memberId" = $(memberId))
    `,
    { memberId },
  )
}
