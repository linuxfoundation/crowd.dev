import { schedulePypiIngest } from '../pypi/schedule'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await schedulePypiIngest()
  await svc.start()
})
