import { getServiceChildLogger } from '@crowd/logging'
import { Edition } from '@crowd/types'

import { API_CONFIG, IS_DEV_ENV, IS_TEST_ENV, SEGMENT_CONFIG } from '../conf'
import SequelizeRepository from '../database/repositories/sequelizeRepository'
import { CROWD_ANALYTICS_PLATORM_NAME } from './addProductDataToCrowdTenant'
import getTenatUser from './trackHelper'

const log = getServiceChildLogger('segment')

export default async function identify(
  event,
  properties,
  options: any,
  userId: any = false,
  timestamp: any = false,
) {
  const userEmail = SequelizeRepository.getCurrentUser({
    ...options,
  }).email
  if (
    !IS_TEST_ENV &&
    !IS_DEV_ENV &&
    SEGMENT_CONFIG.writeKey &&
    // This is only for events in the hosted version. Self-hosted has less telemetry.
    (API_CONFIG.edition === Edition.CROWD_HOSTED || API_CONFIG.edition === Edition.LFX) &&
    userEmail !== 'help@crowd.dev'
  ) {
    if (
      properties &&
      properties?.platform &&
      properties?.platform === CROWD_ANALYTICS_PLATORM_NAME
    ) {
      // no need to track crowd analytics events in segment
      // and this is also to ensure we don't get into an infinite loop
      return
    }

    const Analytics = require('analytics-node')
    const analytics = new Analytics(SEGMENT_CONFIG.writeKey)

    const { userIdOut, tenantIdOut } = getTenatUser(userId, options)

    if (!userIdOut) {
      return
    }

    const payload = {
      userId: userIdOut,
      event,
      properties,
      context: {
        groupId: tenantIdOut,
      },
      ...(timestamp && { timestamp }),
    }

    try {
      analytics.track(payload)

      // send product analytics data to crowd tenant workspace
      // await addProductData({
      //   userId: userIdOut,
      //   tenantId: tenantIdOut,
      //   event,
      //   timestamp,
      //   properties,
      // })
    } catch (error) {
      log.error(error, { payload }, 'Could not send the following payload to Segment')
    }
  }
}
