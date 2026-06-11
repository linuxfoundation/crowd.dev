import { Config } from '@crowd/archetype-standard'
import { Options, ServiceWorker } from '@crowd/archetype-worker'

import {
  scheduleMemberSegmentsAggCleanup,
  scheduleMembersCleanup,
  scheduleOrganizationSegmentAggCleanup,
  scheduleOrganizationsCleanup,
} from './schedules/scheduleCleanup'

const config: Config = {
  envvars: [
    'CROWD_TINYBIRD_ACCESS_TOKEN',
    'CROWD_API_SERVICE_URL',
    'CROWD_API_SERVICE_USER_TOKEN',
    'CROWD_LF_AGENT_USER_ID',
  ],
  producer: {
    enabled: false,
  },
  temporal: {
    enabled: true,
  },
  redis: {
    enabled: true,
  },
}

const options: Options = {
  postgres: {
    enabled: true,
  },
  opensearch: {
    enabled: true,
  },
  queue: {
    enabled: true,
  },
}

export const svc = new ServiceWorker(config, options)

setImmediate(async () => {
  await svc.init()

  await scheduleMembersCleanup()
  await scheduleOrganizationsCleanup()
  await scheduleMemberSegmentsAggCleanup()
  await scheduleOrganizationSegmentAggCleanup()

  await svc.start()
})
