import { getCountryName } from '@crowd/common'

import { QueryExecutor } from '../queryExecutor'
import {
  ICountryScanCandidate,
  ICountryScanWindow,
  ICountryUpdate,
  IMemberCountryHints,
} from './types'

export async function findMembersWithoutCountry(
  qx: QueryExecutor,
  { since, until, after }: ICountryScanWindow,
  limit: number,
): Promise<ICountryScanCandidate[]> {
  return qx.select(
    `
      select
        id,
        attributes -> 'location' ->> 'default' as location,
        "updatedAt"::text as "updatedAt"
      from members
      where "updatedAt" >= $(since)
        and "updatedAt" < $(until)
        ${after ? 'and ("updatedAt", id) > ($(after.updatedAt)::timestamptz, $(after.id)::uuid)' : ''}
        and "deletedAt" is null
        and nullif(trim(attributes -> 'location' ->> 'default'), '') is not null
        and not exists (
          select 1
          from jsonb_each_text(coalesce(attributes -> 'country', '{}'::jsonb)) c
          where nullif(trim(c.value), '') is not null
        )
      order by "updatedAt", id
      limit $(limit)
    `,
    { since, until, after, limit },
  )
}

export async function findMemberCountryHints(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<Map<string, IMemberCountryHints>> {
  const rows: { memberId: string; orgCountry: string | null; emailDomain: string | null }[] =
    await qx.select(
      `
        select mo."memberId", o.country as "orgCountry", null as "emailDomain"
        from "memberOrganizations" mo
        join organizations o on o.id = mo."organizationId"
        where mo."memberId" in ($(memberIds:csv))
          and mo."deletedAt" is null
          and mo."dateEnd" is null
          and o."deletedAt" is null
          and nullif(trim(o.country), '') is not null
        union all
        select "memberId", null, lower(split_part(value, '@', 2))
        from "memberIdentities"
        where "memberId" in ($(memberIds:csv))
          and type = 'email'
          and verified
          and "deletedAt" is null
      `,
      { memberIds },
    )

  const hints = new Map<string, IMemberCountryHints>()
  for (const { memberId, orgCountry, emailDomain } of rows) {
    const memberHints = hints.get(memberId) ?? { orgCountries: [], emailDomains: [] }
    if (orgCountry) memberHints.orgCountries.push(orgCountry)
    if (emailDomain) memberHints.emailDomains.push(emailDomain)
    hints.set(memberId, memberHints)
  }

  return hints
}

export async function saveMemberCountries(
  qx: QueryExecutor,
  updates: ICountryUpdate[],
): Promise<void> {
  if (updates.length === 0) {
    return
  }

  await qx.result(
    `
      update members m
      set attributes = m.attributes || jsonb_build_object(
            'country', jsonb_build_object(u.source, u.country, 'default', u.country),
            'geo', jsonb_build_object(u.source, u.geo, 'default', u.geo)
          ),
          "updatedAt" = now()
      from unnest($(ids)::uuid[], $(sources)::text[], $(countries)::text[], $(geos)::jsonb[])
        as u(id, source, country, geo)
      where m.id = u.id
        and not exists (
          select 1
          from jsonb_each_text(coalesce(m.attributes -> 'country', '{}'::jsonb)) c
          where nullif(trim(c.value), '') is not null
        )
    `,
    {
      ids: updates.map((u) => u.id),
      sources: updates.map((u) => u.source),
      countries: updates.map((u) => getCountryName(u.countryCode)),
      geos: updates.map((u) => JSON.stringify({ countryCode: u.countryCode })),
    },
  )
}
