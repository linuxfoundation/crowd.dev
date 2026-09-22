import { Config } from '@crowd/archetype-standard'
import { Options, ServiceWorker } from '@crowd/archetype-worker'
import { IS_DEV_ENV } from '@crowd/common'

import {
  scheduleCalculateLeafSegmentAggregates,
  scheduleRefreshMemberDisplayAggregates,
  scheduleRefreshOrganizationDisplayAggregates,
} from './schedules/refreshDisplayAggregates'

const config: Config = {
  envvars: [
    'CROWD_AWS_BEDROCK_ACCESS_KEY_ID',
    'CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY',
    'CROWD_API_SERVICE_URL',
    'CROWD_LF_AGENT_USER_TOKEN',
    'CROWD_SEARCH_SYNC_API_URL',
  ],
  producer: {
    enabled: false,
  },
  temporal: {
    enabled: true,
  },
  redis: {
    enabled: false,
  },
}

const options: Options = {
  postgres: {
    enabled: true,
  },
  opensearch: {
    enabled: false,
  },
  queue: {
    enabled: true,
  },
}

export const svc = new ServiceWorker(config, options)

setImmediate(async () => {
  await svc.init()

  // Aggregate calculation schedules
  if (IS_DEV_ENV) {
    await scheduleCalculateLeafSegmentAggregates() // Every 5 minutes - calculates leaf/subproject aggregates
    await scheduleRefreshMemberDisplayAggregates() // Every 10 minutes - rolls up to project/project-group
    await scheduleRefreshOrganizationDisplayAggregates() // Every 10 minutes - rolls up to project/project-group
  }

  await svc.start()
})
