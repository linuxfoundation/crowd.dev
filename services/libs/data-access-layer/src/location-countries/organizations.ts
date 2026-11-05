import { QueryExecutor } from '../queryExecutor'
import { ICountryScanCandidate, ICountryScanWindow, ICountryUpdate } from './types'

export async function findOrganizationsWithoutCountry(
  qx: QueryExecutor,
  { since, until, after }: ICountryScanWindow,
  limit: number,
): Promise<ICountryScanCandidate[]> {
  return qx.select(
    `
      select o.id, o.location, o."updatedAt"::text as "updatedAt"
      from organizations o
      where o."updatedAt" >= $(since)
        and o."updatedAt" < $(until)
        ${after ? 'and (o."updatedAt", o.id) > ($(after.updatedAt)::timestamptz, $(after.id)::uuid)' : ''}
        and o."deletedAt" is null
        and nullif(trim(o.location), '') is not null
        and nullif(trim(o.country), '') is null
        and not exists (
          select 1
          from "orgAttributes" a
          where a."organizationId" = o.id
            and a.name = 'country'
            and nullif(trim(a.value), '') is not null
        )
      order by o."updatedAt", o.id
      limit $(limit)
    `,
    { since, until, after, limit },
  )
}

export async function saveOrganizationCountries(
  qx: QueryExecutor,
  updates: ICountryUpdate[],
): Promise<void> {
  if (updates.length === 0) {
    return
  }

  await qx.result(
    `
      with updated as (
        update organizations o
        set country = u.country,
            "updatedAt" = now()
        from unnest($(ids)::uuid[], $(sources)::text[], $(countries)::text[]) as u(id, source, country)
        where o.id = u.id
          and nullif(trim(o.country), '') is null
        returning o.id, u.source, u.country
      )
      insert into "orgAttributes" ("organizationId", name, source, "default", value)
      select id, 'country', source, true, country
      from updated
      on conflict ("organizationId", name, source, md5(value)) do nothing
    `,
    {
      ids: updates.map((u) => u.id),
      sources: updates.map((u) => u.source),
      countries: updates.map((u) => u.country),
    },
  )
}
