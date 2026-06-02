import {
  scheduleDailyDownloadsBackfill,
  scheduleLast30dDownloadsRefresh,
  scheduleNpmIngest,
} from '../npm/schedule'
import { scheduleOsvSync } from '../osv/schedule'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await scheduleNpmIngest()
  await scheduleDailyDownloadsBackfill()
  await scheduleLast30dDownloadsRefresh()
  await scheduleOsvSync()
  await svc.start()
})
