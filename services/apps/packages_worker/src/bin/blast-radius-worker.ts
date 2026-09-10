import { Config } from '@crowd/archetype-standard'
import { Options, ServiceWorker } from '@crowd/archetype-worker'
import { SlackChannel } from '@crowd/slack'

// Own ServiceWorker instance rather than the shared one in ../service (used by every
// other packages_worker entry point) — its activities are CPU/IO-heavy (tarball
// downloads+extraction, agent subprocesses), so it needs a concurrency cap that would
// otherwise throttle unrelated, lightweight ingestion workers if set on the shared
// instance. Left uncapped, the SDK default lets dozens run at once in this one process,
// starving the event loop and missing heartbeat deadlines.
const config: Config = {
  envvars: [],
  producer: { enabled: false },
  temporal: { enabled: true },
  redis: { enabled: false },
}

const options: Options = {
  postgres: { enabled: false }, // packages-db is managed via getPackagesDb()
  maxConcurrentActivityTaskExecutions: 16,
  alertChannel: SlackChannel.CDP_AKRITES_ALERTS,
}

const svc = new ServiceWorker(config, options)

// On-demand only — analyzeBlastRadius is triggered per request from the backend's
// submitBlastRadiusJob handler (workflow.start), not on a schedule.
setImmediate(async () => {
  await svc.init()
  await svc.start()
})
