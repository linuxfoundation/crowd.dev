import { scheduleSecurityContactsIngestion } from '../security-contacts/schedule'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await scheduleSecurityContactsIngestion()
  await svc.start()
})
