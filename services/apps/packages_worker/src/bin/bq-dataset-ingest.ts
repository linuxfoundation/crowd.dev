import { scheduleOsspckgsBootstrap } from '../deps-dev/schedules/bootstrap'
import { scheduleOsspckgsCleanup } from '../schedules/cleanup'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await scheduleOsspckgsBootstrap()
  await scheduleOsspckgsCleanup()
  await svc.start()
})
