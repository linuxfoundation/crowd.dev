import { Config } from '@crowd/archetype-standard'
import { Options, ServiceWorker } from '@crowd/archetype-worker'

import { scheduleCaptureStarSnapshots } from './schedules/scheduleCaptureStarSnapshots'

const config: Config = {
  envvars: ['GITHUB_TOKEN_CLIENT_ID', 'GITHUB_TOKEN_INSTALLATION_ID', 'GITHUB_TOKEN_PRIVATE_KEY'],
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

  await scheduleCaptureStarSnapshots()
  await svc.start()
})
