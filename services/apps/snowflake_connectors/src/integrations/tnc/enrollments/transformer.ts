import { TNC_GRID, TncActivityType } from '@crowd/integrations'
import { getServiceChildLogger } from '@crowd/logging'
import { IActivityData, MemberAttributeName, PlatformType } from '@crowd/types'

import { TransformedActivity } from '../../../core/transformerBase'
import { TncTransformerBase } from '../tncTransformerBase'

const log = getServiceChildLogger('tncEnrollmentsTransformer')

export class TncEnrollmentsTransformer extends TncTransformerBase {
  transformRow(row: Record<string, unknown>): TransformedActivity | null {
    const email = (row.USER_EMAIL as string | null)?.trim() || null
    if (!email) {
      log.warn(
        { enrollmentId: row.ENROLLMENT_ID, rawUserEmail: row.USER_EMAIL },
        'Skipping row: missing email',
      )
      return null
    }

    const enrollmentId = (row.ENROLLMENT_ID as string)?.trim()
    const learnerName = (row.LEARNER_NAME as string | null)?.trim() || null
    const lfUsername = (row.LFID as string | null)?.trim() || null
    const sourceId = (row.USER_ID as string | null) || undefined

    const identities = this.buildMemberIdentities({
      email,
      sourceId,
      platformUsername: null,
      lfUsername,
    })

    const productType = (row.PRODUCT_TYPE as string | null)?.trim() || null
    const instructionType = (row.INSTRUCTION_TYPE as string | null)?.trim() || null

    let type: TncActivityType
    if (
      productType?.toLowerCase() === 'certification' &&
      instructionType?.toLowerCase() === 'certification exam'
    ) {
      type = TncActivityType.ENROLLED_CERTIFICATION
    } else if (productType?.toLowerCase() === 'training') {
      type = TncActivityType.ENROLLED_TRAINING
    } else {
      log.warn(
        { enrollmentId, productType, instructionType },
        'Skipping row: unrecognized product/instruction type',
      )
      return null
    }

    const activity: IActivityData = {
      type,
      platform: PlatformType.TNC,
      timestamp: (row.ENROLLMENT_TS as string | null) || null,
      score: TNC_GRID[type].score,
      sourceId: enrollmentId,
      sourceParentId: (row.COURSE_ID as string | null) || undefined,
      member: {
        displayName: learnerName || email.split('@')[0],
        identities,
        organizations: this.buildOrganizations(row),
        attributes: {
          ...((row.LEARNER_TITLE as string | null) && {
            [MemberAttributeName.JOB_TITLE]: { [PlatformType.TNC]: row.LEARNER_TITLE as string },
          }),
          ...((row.USER_COUNTRY as string | null) && {
            [MemberAttributeName.COUNTRY]: { [PlatformType.TNC]: row.USER_COUNTRY as string },
          }),
        },
      },
      attributes: {
        productName: (row.COURSE_NAME as string | null) || null,
        productType,
        technology: (row.TECHNOLOGIES_LIST as string | null) || null,
        parentProduct: (row.COURSE_GROUP_ID as string | null) || null,
        courseCode: (row.COURSE_CODE as string | null) || null,
        instructionType,
        location: (row.LOCATION as string | null) || null,
        courseStatus: (row.COURSE_STATUS as string | null) || null,
        courseStartedDate: (row.COURSE_STARTED_DATE as string | null) || null,
        courseCompletedDate: (row.COURSE_COMPLETED_DATE as string | null) || null,
      },
    }

    const segmentSlug = (row.PROJECT_SLUG as string | null)?.trim() || null
    const segmentSourceId = (row.PROJECT_ID as string | null)?.trim() || null
    if (!segmentSlug || !segmentSourceId) {
      log.warn(
        {
          enrollmentId,
          segmentSlug,
          segmentSourceId,
          rawProjectSlug: row.PROJECT_SLUG,
          rawProjectId: row.PROJECT_ID,
        },
        'Skipping row: missing segment slug or sourceId',
      )
      return null
    }

    return { activity, segment: { slug: segmentSlug, sourceId: segmentSourceId } }
  }
}
