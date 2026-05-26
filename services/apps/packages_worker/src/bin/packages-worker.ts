import { scheduleNpmIngest } from '../npm/schedule'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await scheduleNpmIngest()
  await svc.start()
})
