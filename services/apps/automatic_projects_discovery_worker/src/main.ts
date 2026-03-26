import { Config } from '@crowd/archetype-standard'
import { Options, ServiceWorker } from '@crowd/archetype-worker'

import { scheduleProjectsDiscovery } from './schedules/scheduleProjectsDiscovery'

const config: Config = {
  envvars: [],
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
