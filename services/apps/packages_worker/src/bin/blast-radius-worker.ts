import { svc } from '../service'

// On-demand only — analyzeBlastRadius is triggered per request from the backend's
// submitBlastRadiusJob handler (workflow.start), not on a schedule.
setImmediate(async () => {
  await svc.init()
  await svc.start()
})
