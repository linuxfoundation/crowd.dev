import { scheduleOsspckgsBootstrap } from '../deps-dev/schedules/bootstrap'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await scheduleOsspckgsBootstrap()
  await svc.start()
})
