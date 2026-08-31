import { Config } from '@crowd/archetype-standard'
import { Options, ServiceWorker } from '@crowd/archetype-worker'

import { scheduleProjectsOnboarding } from './schedules/scheduleProjectsOnboarding'

const config: Config = {
  envvars: [
    'CROWD_API_SERVICE_URL',
    'CROWD_LF_AGENT_USER_TOKEN',
    'CROWD_GITHUB_PERSONAL_ACCESS_TOKENS',
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
}

export const svc = new ServiceWorker(config, options)

setImmediate(async () => {
  await svc.init()

  svc.log.info('Automatic onboarding worker starting up.')

  await scheduleProjectsOnboarding()

  svc.log.info('Automatic onboarding worker running — schedule registered, waiting for Temporal.')

  await svc.start()
})
