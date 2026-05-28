import { scheduleNpmIngest } from '../npm/schedule'
import { scheduleOsvSync } from '../osv/schedule'
import { schedulePomFetcher } from '../pom-fetcher/schedule'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await scheduleNpmIngest()
  await scheduleOsvSync()
  await schedulePomFetcher()
  await svc.start()
})
