/* eslint-disable @typescript-eslint/no-explicit-any */
import { INangoRecord } from './types'

export const toRecord = (data: any): INangoRecord => {
  return {
    id: data.id,
    timestamp: Date.parse(data.activityData?.timestamp ?? data.timestamp ?? ''),
    integrationId: data.integrationId,

    activity: data.activityData,

    metadata: {
      deletedAt: data._nango_metadata.deleted_at,
      lastAction: data._nango_metadata.last_action,
      firstSeenAt: data._nango_metadata.first_seen_at,
      cursor: data._nango_metadata.cursor,
      lastModifiedAt: data._nango_metadata.last_modified_at,
    },

    rawPayload: data,
  }
}
