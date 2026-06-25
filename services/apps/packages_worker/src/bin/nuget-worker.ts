import { scheduleNuGetIngestion } from '../nuget/schedule'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await scheduleNuGetIngestion()
  await svc.start()
})
