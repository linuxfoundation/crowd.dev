import { DbStore, RepositoryBase } from '@crowd/database'
import { Logger } from '@crowd/logging'
import { IMember, IMemberAttribute, PlatformType } from '@crowd/types'

import { IMemberIdWithAttributes } from './member.data'

export interface IMemberIdObject {
  memberId: string
}

export class MemberRepository extends RepositoryBase<MemberRepository> {
  constructor(dbStore: DbStore, parentLog: Logger) {
    super(dbStore, parentLog)
  }

  public async getTenantMemberAttributes(tenantId: string): Promise<IMemberAttribute[]> {
    const results = await this.db().any(
      `select type, "canDelete", show, label, name, options from "memberAttributeSettings" where "tenantId" = $(tenantId)`,
      {
        tenantId,
      },
    )

    return results
  }

  public async getExistingPlatforms(): Promise<PlatformType[]> {
    const results = await this.db().any(
      `select distinct platform from "memberIdentities" where "deletedAt" is null;`,
    )

    return results.map((p) => p.platform)
  }

  public async findMemberAttributes(memberId: string): Promise<IMemberIdWithAttributes> {
    return await this.db().oneOrNone(`select id, attributes from members where id = $(memberId)`, {
      memberId,
    })
  }

  public async findOrganizationMembers(
    organizationId: string,
    limit: number,
    offset: number,
  ): Promise<IMemberIdObject[]> {
    const results = await this.db().any(
      `select distinct "memberId"
        from "memberOrganizations"
        where "organizationId" = $(organizationId)
          and "deletedAt" is null
        order by "memberId"
        limit $(limit) offset $(offset)`,
      {
        organizationId,
        limit,
        offset,
      },
    )
    return results
  }

  public async findMember(id: string): Promise<IMember> {
    return await this.db().oneOrNone(
      `
      with to_merge_data as (select mtm."memberId",
                                      array_agg(distinct mtm."toMergeId"::text) as to_merge_ids
                              from "memberToMerge" mtm
                                        inner join members m2 on mtm."toMergeId" = m2.id
                              where mtm."memberId" = $(id)
                                and m2."deletedAt" is null
                              group by mtm."memberId"),
            no_merge_data as (select mnm."memberId",
                                      array_agg(distinct mnm."noMergeId"::text) as no_merge_ids
                              from "memberNoMerge" mnm
                                        inner join members m2 on mnm."noMergeId" = m2.id
                              where mnm."memberId" = $(id)
                                and m2."deletedAt" is null
                              group by mnm."memberId"),
            member_organizations as (select mo."memberId",
                                            os."segmentId",
                                            json_agg(
                                                    json_build_object(
                                                            'id', o.id,
                                                            'logo', o.logo,
                                                            'displayName', o."displayName"
                                                        )
                                                )           as all_organizations,
                                            jsonb_agg(o.id) as all_ids
                                      from "memberOrganizations" mo
                                              inner join organizations o on mo."organizationId" = o.id
                                              inner join "organizationSegments" os on o.id = os."organizationId"
                                      where mo."memberId" = $(id)
                                        and o."deletedAt" is null
                                        and mo."deletedAt" is null
                                      group by mo."memberId", os."segmentId"),
            identities as (select mi."memberId",
                                  json_agg(
                                          json_build_object(
                                                  'platform', mi.platform,
                                                  'value', mi.value,
                                                  'type', mi.type
                                              )
                                      ) as identities
                            from "memberIdentities" mi
                            where mi."memberId" = $(id) and mi."deletedAt" is null
                            group by mi."memberId"),
            activity_data as (select a."memberId",
                                      a."segmentId",
                                      count(a.id)                                                          as "activityCount",
                                      max(a.timestamp)                                                     as "lastActive",
                                      array_agg(distinct concat(a.platform, ':', a.type))
                                      filter (where a.platform is not null)                                as "activityTypes",
                                      array_agg(distinct a.platform) filter (where a.platform is not null) as "activeOn",
                                      count(distinct a."timestamp"::date)                                  as "activeDaysCount",
                                      round(avg(
                                                    case
                                                        when (a.sentiment ->> 'sentiment'::text) is not null
                                                            then (a.sentiment ->> 'sentiment'::text)::double precision
                                                        else null::double precision
                                                        end)::numeric, 2)                                  as "averageSentiment"
                              from activities a
                              where a."memberId" = $(id)
                              group by a."memberId", a."segmentId")
        select m.id,
              m."tenantId",
              ms."segmentId",
              m."displayName",
              m.attributes,
              m.score,
              m."joinedAt",
              m."createdAt",
              (m.reach -> 'total')::integer                      as "totalReach",
              CASE WHEN jsonb_typeof(m.contributions) = 'array' THEN jsonb_array_length(m.contributions) ELSE 0 END as "numberOfOpenSourceContributions",

              ad."activeOn",
              ad."activityCount",
              ad."activityTypes",
              ad."activeDaysCount",
              ad."lastActive",
              ad."averageSentiment",

              i.identities,
              coalesce(mo.all_organizations, json_build_array()) as organizations,
              coalesce(tmd.to_merge_ids, array []::text[])       as "toMergeIds",
              coalesce(nmd.no_merge_ids, array []::text[])       as "noMergeIds"
        from "memberSegments" ms
                inner join members m on ms."memberId" = m.id
                inner join identities i on m.id = i."memberId"
                inner join activity_data ad on ms."memberId" = ad."memberId" and ms."segmentId" = ad."segmentId"
                left join to_merge_data tmd on m.id = tmd."memberId"
                left join no_merge_data nmd on m.id = nmd."memberId"
                left join member_organizations mo on ms."memberId" = mo."memberId" and ms."segmentId" = mo."segmentId"
        where ms."memberId" = $(id)
          and m."deletedAt" is null;`,
      {
        id,
      },
    )
  }
}
