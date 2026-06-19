import { scheduleCargoSync } from '../cargo/schedule'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await scheduleCargoSync()
  await svc.start()
})
