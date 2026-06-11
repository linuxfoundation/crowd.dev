import { Config } from '@crowd/archetype-standard'
import { Options, ServiceWorker } from '@crowd/archetype-worker'

import { scheduleGenerateMemberMergeSuggestions } from './schedules/memberMergeSuggestions'
import { scheduleGenerateOrganizationMergeSuggestions } from './schedules/organizationMergeSuggestions'

const config: Config = {
  envvars: [
    'CROWD_AWS_BEDROCK_ACCESS_KEY_ID',
    'CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY',
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
    enabled: false,
  },
}

const options: Options = {
  postgres: {
    enabled: true,
  },
  opensearch: {
    enabled: true,
  },
}

export const svc = new ServiceWorker(config, options)

setImmediate(async () => {
  await svc.init()

  await scheduleGenerateMemberMergeSuggestions()
  await scheduleGenerateOrganizationMergeSuggestions()

  await svc.start()
})
