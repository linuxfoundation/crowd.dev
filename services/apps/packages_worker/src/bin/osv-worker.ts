import { scheduleOsvSync } from '../osv/schedule'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await scheduleOsvSync()
  await svc.start()
})
