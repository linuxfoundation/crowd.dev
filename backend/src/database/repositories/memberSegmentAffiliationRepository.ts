import { QueryTypes } from 'sequelize'
import { v4 as uuid } from 'uuid'

import { captureApiChange, memberEditAffiliationsAction } from '@crowd/audit-logs'
import { Error404 } from '@crowd/common'
import {
  deleteMemberAffiliations,
  findMemberAffiliations,
  insertMemberAffiliations,
} from '@crowd/data-access-layer/src/member_segment_affiliations'
import {
  MemberSegmentAffiliation,
  MemberSegmentAffiliationCreate,
  MemberSegmentAffiliationUpdate,
} from '@crowd/types'

import { IRepositoryOptions } from './IRepositoryOptions'
import { RepositoryBase } from './repositoryBase'
import SequelizeRepository from './sequelizeRepository'

class MemberSegmentAffiliationRepository extends RepositoryBase<
  MemberSegmentAffiliation,
  string,
  MemberSegmentAffiliationCreate,
  MemberSegmentAffiliationUpdate,
  unknown
> {
  public constructor(options: IRepositoryOptions) {
    super(options, true)
  }

  async createOrUpdate(data: MemberSegmentAffiliationCreate): Promise<MemberSegmentAffiliation> {
    if (!data.memberId) {
      throw new Error('memberId is required when creating a member segment affiliation.')
    }

    if (!data.segmentId) {
      throw new Error('segmentId is required when creating a member segment affiliation.')
    }

    if (data.organizationId === undefined) {
      throw new Error('organizationId is required when creating a member segment affiliation.')
    }

    const transaction = this.transaction

    const affiliationInsertResult = await this.options.database.sequelize.query(
      `INSERT INTO "memberSegmentAffiliations" ("id", "memberId", "segmentId", "organizationId", "dateStart", "dateEnd")
          VALUES
              (:id, :memberId, :segmentId, :organizationId, :dateStart, :dateEnd)
          ON CONFLICT (
              "memberId",
              "segmentId",
              COALESCE("organizationId", '00000000-0000-0000-0000-000000000000'::uuid),
              COALESCE("dateStart", '1970-01-01T00:00:00Z'::timestamptz),
              COALESCE("dateEnd", '1970-01-01T00:00:00Z'::timestamptz)
          )
          DO UPDATE SET "organizationId" = EXCLUDED."organizationId"
          RETURNING "id"
        `,
      {
        replacements: {
          id: uuid(),
          memberId: data.memberId,
          segmentId: data.segmentId,
          organizationId: data.organizationId,
          dateStart: data.dateStart || null,
          dateEnd: data.dateEnd || null,
        },
        type: QueryTypes.INSERT,
        transaction,
      },
    )

    await this.updateAffiliation(data.memberId, data.segmentId, data.organizationId)

    return this.findById(affiliationInsertResult[0][0].id)
  }

  async setForMember(memberId: string, data: MemberSegmentAffiliationCreate[]): Promise<void> {
    const qx = SequelizeRepository.getQueryExecutor(this.options)

    await captureApiChange(
      this.options,
      memberEditAffiliationsAction(memberId, async (captureOldState, captureNewState) => {
        const oldOnes = await findMemberAffiliations(qx, memberId)
        captureOldState(
          oldOnes.map((item) => ({
            segmentId: item.segmentId,
            organizationId: item.organizationId,
            dateStart: item.dateStart,
            dateEnd: item.dateEnd,
          })),
        )

        captureNewState(data)

        await deleteMemberAffiliations(qx, memberId)

        if (data.length === 0) {
          return
        }

        await insertMemberAffiliations(qx, memberId, data)
      }),
    )
  }

  override async findById(id: string): Promise<MemberSegmentAffiliation> {
    const transaction = this.transaction

    const records = await this.options.database.sequelize.query(
      `SELECT *
       FROM "memberSegmentAffiliations"
       WHERE id = :id
      `,
      {
        replacements: {
          id,
        },
        type: QueryTypes.SELECT,
        transaction,
      },
    )

    if (records.length === 0) {
      return null
    }

    return records[0]
  }

  override async update(
    id: string,
    data: MemberSegmentAffiliationUpdate,
  ): Promise<MemberSegmentAffiliation> {
    const transaction = this.transaction

    if (data.organizationId === undefined) {
      throw new Error('When updating member segment affiliation, organizationId is required.')
    }

    const affiliation = await this.findById(id)

    if (!affiliation) {
      throw new Error404()
    }

    await this.options.database.sequelize.query(
      `UPDATE "memberSegmentAffiliations" SET "organizationId" = :organizationId`,
      {
        replacements: {
          organizationId: data.organizationId,
        },
        type: QueryTypes.UPDATE,
        transaction,
      },
    )

    return this.findById(id)
  }

  override async destroyAll(ids: string[]): Promise<void> {
    const transaction = this.transaction

    const records = await this.findInIds(ids)

    if (ids.some((id) => records.find((r) => r.id === id) === undefined)) {
      throw new Error404()
    }

    await this.options.database.sequelize.query(
      `DELETE FROM "memberSegmentAffiliations" WHERE id in (:ids)
              `,
      {
        replacements: {
          ids,
        },
        type: QueryTypes.SELECT,
        transaction,
      },
    )
  }

  async findInIds(ids: string[]): Promise<MemberSegmentAffiliation[]> {
    const transaction = this.transaction

    const records = await this.options.database.sequelize.query(
      `SELECT *
             FROM "memberSegmentAffiliations"
             WHERE id in (:ids)
            `,
      {
        replacements: {
          ids,
        },
        type: QueryTypes.SELECT,
        transaction,
      },
    )

    return records
  }

  async findForMember(memberId: string, timestamp: string): Promise<MemberSegmentAffiliation> {
    const transaction = SequelizeRepository.getTransaction(this.options)

    const segment = SequelizeRepository.getStrictlySingleActiveSegment(this.options)

    const seq = SequelizeRepository.getSequelize(this.options)

    const records = await seq.query(
      `
        SELECT * FROM "memberSegmentAffiliations"
        WHERE "memberId" = :memberId
          AND "segmentId" = :segmentId
          AND (
            ("dateStart" <= :timestamp AND "dateEnd" >= :timestamp)
            OR ("dateStart" <= :timestamp AND "dateEnd" IS NULL)
          )
        ORDER BY "dateStart" DESC, id
        LIMIT 1
      `,
      {
        replacements: {
          memberId,
          segmentId: segment.id,
          timestamp,
        },
        type: QueryTypes.SELECT,
        transaction,
      },
    )

    if (records.length === 0) {
      return null
    }

    return records[0] as MemberSegmentAffiliation
  }

  private async updateAffiliation(memberId, segmentId, organizationId) {
    const transaction = this.transaction

    const query = `
      UPDATE activities
      SET "organizationId" = :organizationId
      WHERE "memberId" = :memberId
        AND "segmentId" = :segmentId
    `

    await this.options.database.sequelize.query(query, {
      replacements: {
        memberId,
        segmentId,
        organizationId,
      },
      type: QueryTypes.UPDATE,
      transaction,
    })
  }
}

export default MemberSegmentAffiliationRepository
