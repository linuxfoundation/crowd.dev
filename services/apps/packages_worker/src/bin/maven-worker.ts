import { scheduleMavenCritical } from '../maven/schedule'
import { svc } from '../service'

// Maven-only worker: runs on the shared `packages-worker` taskqueue (so it picks up
// the same bundled workflows/activities) but registers ONLY the maven-critical
// schedule. Intended for local dev — lets you run Maven in isolation without also
// firing the npm/osv schedules that bin/packages-worker.ts registers.
setImmediate(async () => {
  await svc.init()
  await scheduleMavenCritical()
  await svc.start()
})
