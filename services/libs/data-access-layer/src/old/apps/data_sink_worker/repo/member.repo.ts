import { DEFAULT_TENANT_ID } from '@crowd/common'
import { DbColumnSet, DbStore, RepositoryBase } from '@crowd/database'
import { Logger } from '@crowd/logging'
import { IMemberIdentity, MemberIdentityType } from '@crowd/types'

import {
  deleteManyMemberIdentities,
  insertManyMemberIdentities,
} from '../../../../members/identities'
import { PgPromiseQueryExecutor } from '../../../../queryExecutor'

import { IDbMember, getInsertMemberColumnSet, getSelectMemberColumnSet } from './member.data'

export default class MemberRepository extends RepositoryBase<MemberRepository> {
  private readonly insertMemberColumnSet: DbColumnSet
  private readonly selectMemberColumnSet: DbColumnSet

  constructor(dbStore: DbStore, parentLog: Logger) {
    super(dbStore, parentLog)

    this.insertMemberColumnSet = getInsertMemberColumnSet(this.dbInstance)
    this.selectMemberColumnSet = getSelectMemberColumnSet(this.dbInstance)
  }

  public async findByIds(ids: string[]): Promise<IDbMember[]> {
    return await this.db().any(
      `
      select ${this.selectMemberColumnSet.columns.map((c) => `m."${c.name}"`).join(', ')}
      from "members" m
      where m.id in ($(ids:csv))
    `,
      { ids },
    )
  }

  public async removeIdentities(memberId: string, identities: IMemberIdentity[]): Promise<void> {
    const rowCount = await deleteManyMemberIdentities(new PgPromiseQueryExecutor(this.db()), {
      memberId,
      identities,
    })

    this.checkUpdateRowCount(rowCount, identities.length)
  }

  public async updateIdentities(memberId: string, identities: IMemberIdentity[]): Promise<void> {
    const objects = identities.map((i) => {
      return {
        memberId,
        platform: i.platform,
        value: i.value,
        type: i.type,
        verified: i.verified,
      }
    })

    const query =
      this.dbInstance.helpers.update(
        objects,
        ['verified', '?memberId', '?platform', '?type', '?value'],
        'memberIdentities',
      ) +
      ' where t."memberId" = v."memberId"::uuid and t.platform = v.platform and t.type = v.type and t.value = v.value and t."deletedAt" is null'

    await this.db().none(query)
  }

  public async destroyMemberAfterError(id: string, clearIdentities = false): Promise<void> {
    if (clearIdentities) {
      await this.db().none(
        `update "memberIdentities" set "deletedAt" = now() where "memberId" = $(id)`,
        {
          id,
        },
      )
    }

    await this.db().none(
      `
      delete from "members" where id = $(id)
      `,
      { id },
    )
  }

  public async insertIdentities(
    memberId: string,
    integrationId: string,
    identities: IMemberIdentity[],
  ): Promise<void> {
    const objects = identities.map((i) => {
      return {
        memberId,
        integrationId,
        platform: i.platform,
        sourceId: i.sourceId,
        value: i.value,
        type: i.type,
        verified: i.verified,
        source: 'integration',
      }
    })

    await insertManyMemberIdentities(new PgPromiseQueryExecutor(this.db()), objects, true)
  }

  public async addToSegments(memberId: string, segmentIds: string[]): Promise<void> {
    if (segmentIds.length === 0) {
      return
    }

    const params: Record<string, unknown> = {
      tenantId: DEFAULT_TENANT_ID,
      memberId,
    }

    const values: string[] = []
    for (let i = 0; i < segmentIds.length; i++) {
      const paramName = `segmentId_${i}`
      params[paramName] = segmentIds[i]
      values.push(`($(memberId), $(tenantId), $(${paramName}))`)
    }

    const query = `
      insert into "memberSegments"("memberId", "tenantId", "segmentId")
      values ${values.join(', ')}
      on conflict do nothing
    `
    await this.db().none(query, params)
  }

  public async getMemberIdsAndEmailsAndCount(
    segmentIds: string[],
    { limit = 20, offset = 0, orderBy = 'joinedAt_DESC', countOnly = false },
  ) {
    let orderByString = ''
    const orderByParts = orderBy.split('_')
    const direction = orderByParts[1].toLowerCase()

    switch (orderByParts[0]) {
      case 'joinedAt':
        orderByString = 'm."joinedAt"'
        break
      case 'displayName':
        orderByString = 'm."displayName"'
        break
      case 'reach':
        orderByString = "(m.reach ->> 'total')::int"
        break
      case 'score':
        orderByString = 'm.score'
        break

      default:
        throw new Error(`Invalid order by: ${orderBy}!`)
    }

    orderByString = `${orderByString} ${direction}`

    const memberCount = await this.db().one(
      `
      SELECT count(*) FROM (
        SELECT m.id
        FROM "members" m
        JOIN "memberSegments" ms ON ms."memberId" = m.id
        WHEREms."segmentId" = ANY($(segmentIds)::uuid[])
      ) as count
      `,
      {
        segmentIds,
      },
    )

    if (countOnly) {
      return {
        totalCount: Number(memberCount.count),
        members: [],
      }
    }

    const members = await this.db().any(
      `
      with member_emails as (
        select mi."memberId", array_agg(mi.value) as emails
        from "memberIdentities" mi
        where mi."deletedAt" is null and mi.verified = true
              and mi.type = '${MemberIdentityType.EMAIL}'
        group by mi."memberId"
      )
      SELECT m.id, me.emails
      FROM "members" m
      JOIN "memberSegments" ms ON ms."memberId" = m.id
      JOIN member_emails me on me."memberId" = m.id
      WHERE ms."segmentId" = ANY($(segmentIds)::uuid[])
      ORDER BY ${orderByString}
      LIMIT $(limit) OFFSET $(offset)
      `,
      {
        segmentIds,
        limit,
        offset,
      },
    )

    return {
      totalCount: Number(memberCount.count),
      members: members,
    }
  }
}
