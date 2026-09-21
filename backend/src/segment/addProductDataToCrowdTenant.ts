import axios from 'axios'

import { getServiceChildLogger } from '@crowd/logging'

import { CROWD_ANALYTICS_CONFIG } from '../conf'
import SequelizeRepository from '../database/repositories/sequelizeRepository'
import TenantRepository from '../database/repositories/tenantRepository'
import UserRepository from '../database/repositories/userRepository'

const IS_CROWD_ANALYTICS_ENABLED = CROWD_ANALYTICS_CONFIG.isEnabled === 'true'
const CROWD_ANALYTICS_TENANT_ID = CROWD_ANALYTICS_CONFIG.tenantId
const CROWD_ANALYTICS_BASE_URL = CROWD_ANALYTICS_CONFIG.baseUrl
const CROWD_ANALYTICS_TOKEN = CROWD_ANALYTICS_CONFIG.apiToken

export const CROWD_ANALYTICS_PLATORM_NAME = 'crowd.dev-analytics'

const log = getServiceChildLogger('segment')

interface CrowdAnalyticsData {
  userId: string
  tenantId: string
  event: string
  timestamp: string
  properties: any
}

const expandAttributes = (attributes: object) => {
  const obj = {}
  Object.keys(attributes).forEach((key) => {
    obj[key.toLowerCase()] = {
      [CROWD_ANALYTICS_PLATORM_NAME]: attributes[key],
    }
  })
  return obj
}

export default async function addProductData(data: CrowdAnalyticsData) {
  if (!IS_CROWD_ANALYTICS_ENABLED) {
    return
  }

  if (!CROWD_ANALYTICS_TENANT_ID) {
    return
  }

  if (!CROWD_ANALYTICS_BASE_URL) {
    return
  }

  if (!CROWD_ANALYTICS_TOKEN) {
    return
  }

  if (!data?.userId) {
    // we can't send data without a user id
    return
  }

  if (!data?.tenantId) {
    // we can't send data without a tenant id
    return
  }

  try {
    const repositoryOptions = await SequelizeRepository.getDefaultIRepositoryOptions()

    const user = await UserRepository.findById(data.userId, repositoryOptions)

    // this is an array of one tenant
    const tenant = await TenantRepository.getTenantInfo(data.tenantId, repositoryOptions)

    const timestamp = data.timestamp || new Date().toISOString()

    const obj = {
      member: {
        username: {
          [CROWD_ANALYTICS_PLATORM_NAME]: user.email,
        },
        emails: [user.email],
        displayName: user.fullName,
        attributes: {
          ...expandAttributes({
            email: user.email,
            createdAnAccount: true,
            firstName: user.firstName,
            lastName: user.lastName,
            plan: tenant[0]?.plan,
            isTrialPlan: tenant[0]?.isTrialPlan,
            trialEndsAt: tenant[0]?.trialEndsAt,
          }),
        },
        organizations: [
          {
            name: tenant[0]?.name,
          },
        ],
      },
      type: data.event,
      timestamp,
      platform: CROWD_ANALYTICS_PLATORM_NAME,
      sourceId: `${data.userId}-${timestamp}-${data.event}`,
    }
    const endpoint = `${CROWD_ANALYTICS_BASE_URL}/tenant/${CROWD_ANALYTICS_TENANT_ID}/activity/with-member`
    await axios.post(endpoint, obj, {
      headers: {
        Authorization: `Bearer ${CROWD_ANALYTICS_TOKEN}`,
      },
    })
  } catch (error) {
    log.error(error, { data }, 'ERROR: Could not send the following payload to Crowd Analytics')
  }
}
