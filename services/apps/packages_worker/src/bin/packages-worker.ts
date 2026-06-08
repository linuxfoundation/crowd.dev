import {
  scheduleDailyDownloadsBackfill,
  scheduleLast30dHistoryBackfill,
  scheduleLatestLast30dRefresh,
  scheduleNpmIngest,
} from '../npm/schedule'
import { scheduleOsvSync } from '../osv/schedule'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await scheduleNpmIngest()
  await scheduleDailyDownloadsBackfill()
  await scheduleLatestLast30dRefresh()
  await scheduleLast30dHistoryBackfill()
  await scheduleOsvSync()
  await svc.start()
})
