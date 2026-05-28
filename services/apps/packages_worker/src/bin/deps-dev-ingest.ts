import { scheduleOsspckgsBootstrap } from '../deps-dev/schedules/bootstrap'
import { svc } from '../service'
import { scheduleOsspckgsCleanup } from '../schedules/cleanup'

setImmediate(async () => {
  await svc.init()
  await scheduleOsspckgsBootstrap()
  await scheduleOsspckgsCleanup()
  await svc.start()
})
