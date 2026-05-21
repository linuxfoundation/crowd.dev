import { Config } from '@crowd/archetype-standard'
import { Options, ServiceWorker } from '@crowd/archetype-worker'

import { scheduleProjectsEvaluation } from './schedules/scheduleProjectsEvaluation'

const config: Config = {
  envvars: [
    'CROWD_PROJECT_EVALUATION_API_ENDPOINT',
    'CROWD_PROJECT_EVALUATION_API_USER_ID',
    'CROWD_PROJECT_EVALUATION_API_SECRET',
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

  svc.log.info('Projects evaluation worker starting up.')

  await scheduleProjectsEvaluation()

  svc.log.info('Projects evaluation worker running — schedule registered, waiting for Temporal.')

  await svc.start()
})
