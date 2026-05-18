import { QueryTypes } from 'sequelize'

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
}

export default MemberSegmentAffiliationRepository
