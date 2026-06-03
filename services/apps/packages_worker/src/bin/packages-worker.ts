import { scheduleMavenCritical } from '../maven/schedule'
import { scheduleNpmIngest } from '../npm/schedule'
import { scheduleOsvSync } from '../osv/schedule'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await scheduleNpmIngest()
  await scheduleOsvSync()
  await scheduleMavenCritical()
  await svc.start()
})
