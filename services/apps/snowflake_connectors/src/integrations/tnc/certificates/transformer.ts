import { TNC_GRID, TncActivityType } from '@crowd/integrations'
import { getServiceChildLogger } from '@crowd/logging'
import { IActivityData, MemberAttributeName, PlatformType } from '@crowd/types'

import { TransformedActivity } from '../../../core/transformerBase'
import { TncTransformerBase } from '../tncTransformerBase'

const log = getServiceChildLogger('tncCertificatesTransformer')

export class TncCertificatesTransformer extends TncTransformerBase {
  transformRow(row: Record<string, unknown>): TransformedActivity | null {
    const email = (row.USER_EMAIL as string | null)?.trim() || null
    if (!email) {
      log.debug({ certificateId: row.CERTIFICATE_ID }, 'Skipping row: missing email')
      return null
    }

    const certificateId = (row.CERTIFICATE_ID as string)?.trim()
    const learnerName = (row.LEARNER_NAME as string | null)?.trim() || null
    const lfUsername = (row.LFID as string | null)?.trim() || null
    const sourceId = (row.USER_ID as string | null) || undefined

    const identities = this.buildMemberIdentities({
      email,
      sourceId,
      platformUsername: null,
      lfUsername,
    })

    const activity: IActivityData = {
      type: TncActivityType.ISSUED_CERTIFICATION,
      platform: PlatformType.TNC,
      timestamp: (row.ISSUED_TS as string | null) || null,
      score: TNC_GRID[TncActivityType.ISSUED_CERTIFICATION].score,
      sourceId: certificateId,
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
        productName: (row.COURSE_NAME as string | null) || null,
        technology: (row.TECHNOLOGIES_LIST as string | null) || null,
        didExpire: row.DID_EXPIRE as boolean | null,
        expirationDate: (row.EXPIRATION_DATE as string | null) || null,
      },
    }

    const segmentSlug = (row.PROJECT_SLUG as string | null)?.trim() || null
    const segmentSourceId = (row.PROJECT_ID as string | null)?.trim() || null
    if (!segmentSlug || !segmentSourceId) {
      return null
    }

    return { activity, segment: { slug: segmentSlug, sourceId: segmentSourceId } }
  }
}
