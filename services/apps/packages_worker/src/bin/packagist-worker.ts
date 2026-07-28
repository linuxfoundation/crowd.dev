import { schedulePackagistIngest } from '../packagist/schedule'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await schedulePackagistIngest()
  await svc.start()
})
