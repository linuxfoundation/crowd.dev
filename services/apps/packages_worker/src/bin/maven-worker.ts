import { scheduleMavenCritical } from '../maven/schedule'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await scheduleMavenCritical()
  await svc.start()
})
