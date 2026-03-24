import { Config } from '@crowd/archetype-standard'
import { Options, ServiceWorker } from '@crowd/archetype-worker'

import {
  scheduleMemberSegmentsAggCleanup,
  scheduleMembersCleanup,
  scheduleOrganizationSegmentAggCleanup,
  scheduleOrganizationsCleanup,
} from './schedules/scheduleCleanup'

const config: Config = {
  envvars: ['CROWD_TINYBIRD_ACCESS_TOKEN'],
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
