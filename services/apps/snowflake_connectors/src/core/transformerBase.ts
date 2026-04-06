/**
 * Abstract transformer class.
 *
 * Each integration must implement this base class to define
 * how raw exported data is transformed into activities.
 */
import { getServiceChildLogger } from '@crowd/logging'
import { IActivityData, IMemberData, MemberIdentityType, PlatformType } from '@crowd/types'

const log = getServiceChildLogger('transformer')

export interface SegmentRef {
  slug: string
  sourceId: string
}

export interface TransformedActivity {
  activity: IActivityData
  segment: SegmentRef
}

export abstract class TransformerBase {
  /** Platform type identifying this transformer / integration. */
  abstract readonly platform: PlatformType

  /**
   * Transform a single raw row from the S3 export into an activity
   * along with routing metadata. Returns null if the row should be skipped.
   */
  abstract transformRow(row: Record<string, unknown>): TransformedActivity | null

  private static readonly INDIVIDUAL_NO_ACCOUNT_RE = /^individual\s*(?:[-–?]|with)\s*no\s+account$/i

  protected isIndividualNoAccount(displayName: string | null): boolean {
    if (!displayName) {
      return false
    }
    return TransformerBase.INDIVIDUAL_NO_ACCOUNT_RE.test(displayName.trim())
  }

  protected buildMemberIdentities(params: {
    email: string
    sourceId?: string
    platformUsername?: string | null
    lfUsername?: string | null
  }): IMemberData['identities'] {
    const { email, sourceId, platformUsername, lfUsername } = params
    const identities: IMemberData['identities'] = [
      {
        platform: this.platform,
        value: email,
        type: MemberIdentityType.EMAIL,
        verified: true,
        verifiedBy: this.platform,
        sourceId,
      },
    ]
    if (!lfUsername && !platformUsername) {
      identities.push({
        platform: this.platform,
        value: email,
        type: MemberIdentityType.USERNAME,
        verified: true,
        verifiedBy: this.platform,
        sourceId,
      })
      return identities
    }

    if (platformUsername) {
      identities.push({
        platform: this.platform,
        value: platformUsername,
        type: MemberIdentityType.USERNAME,
        verified: true,
        verifiedBy: this.platform,
        sourceId,
      })
    }

    if (lfUsername) {
      identities.push({
        platform: PlatformType.LFID,
        value: lfUsername,
        type: MemberIdentityType.USERNAME,
        verified: true,
        verifiedBy: this.platform,
        sourceId,
      })
      if (!platformUsername) {
        identities.push({
          platform: this.platform,
          value: lfUsername,
          type: MemberIdentityType.USERNAME,
          verified: true,
          verifiedBy: this.platform,
          sourceId,
        })
      }
    }

    return identities
  }

  /**
   * Safe wrapper around transformRow that catches errors and returns null.
   */
  safeTransformRow(row: Record<string, unknown>): TransformedActivity | null {
    try {
      return this.transformRow(row)
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      const stack = err instanceof Error ? err.stack : undefined
      log.warn(
        {
          errMessage: message,
          errStack: stack,
          platform: this.platform,
          rowKeys: Object.keys(row),
        },
        'Failed to transform row, skipping',
      )
      return null
    }
  }
}
