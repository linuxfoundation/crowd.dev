import { Config } from '@crowd/archetype-standard'
import { Options, ServiceWorker } from '@crowd/archetype-worker'

import { scheduleProjectsDiscovery } from './schedules/scheduleProjectsDiscovery'

const config: Config = {
  envvars: ['CROWD_GITHUB_PERSONAL_ACCESS_TOKENS', 'LF_CRITICALITY_SCORE_API_URL'],
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
}

export const svc = new ServiceWorker(config, options)

setImmediate(async () => {
  await svc.init()

  await scheduleProjectsDiscovery()

  await svc.start()
})
