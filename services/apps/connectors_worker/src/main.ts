import { Config } from '@crowd/archetype-standard'
import { Options, ServiceWorker } from '@crowd/archetype-worker'
import { IS_DEV_ENV } from '@crowd/common'
import { registerConnector } from '@crowd/connectors'
import { credlyConnector } from '@crowd/connectors/src/connectors/credly'
import { githubConnector } from '@crowd/connectors/src/connectors/github'
import { octolensConnector } from '@crowd/connectors/src/connectors/octolens'
import { dummyConnector } from '@crowd/connectors/src/testing/dummyConnector'

import { scheduleDispatcher } from './schedules/dispatcher'
import { scheduleGithubLifecycleSweep } from './schedules/githubLifecycleSweep'
import { scheduleMirrorSweep } from './schedules/mirrorSweep'
import { scheduleOctolensDiscovery } from './schedules/octolensDiscovery'
import { scheduleShadowDiff } from './schedules/shadowDiff'
import { scheduleSwitchSyncPipelineSweep } from './schedules/switchSyncPipelineSweep'

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

const DRAIN_TIMEOUT_MS = 9 * 60_000

const options: Options = {
  drainTimeoutMs: DRAIN_TIMEOUT_MS,
  postgres: {
    enabled: true,
  },
  opensearch: {
    enabled: false,
  },
}

export const svc = new ServiceWorker(config, options)

registerConnector(githubConnector)
registerConnector(octolensConnector)
registerConnector(credlyConnector)

// POC only: dummy connector drives the control-plane end-to-end in dev
if (IS_DEV_ENV) {
  registerConnector(dummyConnector)
}

setImmediate(async () => {
  await svc.init()

  await scheduleDispatcher()
  await scheduleShadowDiff()
  await scheduleOctolensDiscovery()
  await scheduleSwitchSyncPipelineSweep()
  await scheduleMirrorSweep()
  await scheduleGithubLifecycleSweep()

  await svc.start()
})
