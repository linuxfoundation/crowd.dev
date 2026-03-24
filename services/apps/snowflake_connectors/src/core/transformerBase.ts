/**
 * Abstract transformer class.
 *
 * Each integration must implement this base class to define
 * how raw exported data is transformed into activities.
 */
import { getServiceChildLogger } from '@crowd/logging'
import { IActivityData, PlatformType } from '@crowd/types'

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
