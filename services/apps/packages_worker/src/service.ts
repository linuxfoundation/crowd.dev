import { Config } from '@crowd/archetype-standard'
import { Options, ServiceWorker } from '@crowd/archetype-worker'

const config: Config = {
  envvars: [],
  producer: { enabled: false },
  temporal: { enabled: true },
  redis: { enabled: false },
}

const options: Options = {
  postgres: { enabled: false }, // packages-db is managed via getPackagesDb()
}

export const svc = new ServiceWorker(config, options)
