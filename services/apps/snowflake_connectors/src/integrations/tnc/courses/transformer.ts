import { TNC_GRID, TncActivityType } from '@crowd/integrations'
import { getServiceChildLogger } from '@crowd/logging'
import { IActivityData, MemberAttributeName, PlatformType } from '@crowd/types'

import { TransformedActivity } from '../../../core/transformerBase'
import { TncTransformerBase } from '../tncTransformerBase'

const log = getServiceChildLogger('tncCoursesTransformer')

export class TncCoursesTransformer extends TncTransformerBase {
  transformRow(row: Record<string, unknown>): TransformedActivity | null {
    const email = (row.USER_EMAIL as string | null)?.trim() || null
    if (!email) {
      log.warn(
        { courseActionId: row.COURSE_ACTION_ID, rawUserEmail: row.USER_EMAIL },
        'Skipping row: missing email',
      )
      return null
    }

    const courseActionId = (row.COURSE_ACTION_ID as string | null)?.trim() || null
    if (!courseActionId) {
      log.warn('Skipping row: missing courseActionId')
      return null
    }

    const learnerName = (row.LEARNER_NAME as string | null)?.trim() || null
    const lfUsername = (row.LFID as string | null)?.trim() || null
    const sourceId = (row.INTERNAL_TI_USER_ID as string | null) || undefined

    const identities = this.buildMemberIdentities({
      email,
      sourceId,
      platformUsername: null,
      lfUsername,
    })

    const productType = (row.PRODUCT_TYPE as string | null)?.trim() || null

    let type: TncActivityType
    if (productType?.toLowerCase() === 'training') {
      type = TncActivityType.ATTEMPTED_COURSE
    } else if (productType?.toLowerCase() === 'certification') {
      type = TncActivityType.ATTEMPTED_EXAM
    } else {
      log.warn({ courseActionId, productType }, 'Skipping row: unrecognized product type')
      return null
    }

    const activity: IActivityData = {
      type,
      platform: PlatformType.TNC,
      timestamp: (row.TIMESTAMP as string | null) || null,
      score: TNC_GRID[type].score,
      sourceId: courseActionId,
      sourceParentId: (row.COURSE_ID as string | null) || undefined,
      member: {
        displayName: learnerName || email.split('@')[0],
        identities,
        organizations: this.buildOrganizations(row),
        attributes: {
          ...((row.JOB_TITLE as string | null) && {
            [MemberAttributeName.JOB_TITLE]: { [PlatformType.TNC]: row.JOB_TITLE as string },
          }),
          ...((row.USER_COUNTRY as string | null) && {
            [MemberAttributeName.COUNTRY]: { [PlatformType.TNC]: row.USER_COUNTRY as string },
          }),
        },
      },
      attributes: {
        productName: (row.TITLE as string | null) || null,
        productType: (row.PRODUCT_TYPE as string | null) || null,
        parentProduct: (row.COURSE_GROUP_ID as string | null) || null,
        courseSlug: (row.SLUG as string | null) || null,
        instructionType: (row.INSTRUCTION_TYPE as string | null) || null,
        isCertification: Boolean(row.IS_CERTIFICATION),
        isTraining: Boolean(row.IS_TRAINING),
      },
    }

    const segmentSlug = (row.PROJECT_SLUG as string | null)?.trim() || null
    const segmentSourceId = (row.PROJECT_ID as string | null)?.trim() || null
    if (!segmentSlug || !segmentSourceId) {
      log.warn(
        {
          courseActionId,
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
