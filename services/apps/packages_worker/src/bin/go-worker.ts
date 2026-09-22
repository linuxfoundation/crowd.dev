import { scheduleGoStatus, scheduleGoVersions } from '../go/schedule'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await scheduleGoVersions()
  await scheduleGoStatus()
  await svc.start()
})
