import { Config } from '@crowd/archetype-standard'
import { Options, ServiceWorker } from '@crowd/archetype-worker'
import { IS_DEV_ENV } from '@crowd/common'
import { registerConnector } from '@crowd/connectors'
import { githubConnector } from '@crowd/connectors/src/connectors/github'
import { dummyConnector } from '@crowd/connectors/src/testing/dummyConnector'

import { scheduleDispatcher } from './schedules/dispatcher'

const config: Config = {
  envvars: [],
  producer: {
    enabled: true,
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
    enabled: false,
  },
}

export const svc = new ServiceWorker(config, options)

registerConnector(githubConnector)

// POC only: dummy connector drives the control-plane end-to-end in dev
if (IS_DEV_ENV) {
  registerConnector(dummyConnector)
}

setImmediate(async () => {
  await svc.init()

  await scheduleDispatcher()

  await svc.start()
})
