import {
  scheduleDailyDownloadsBackfill,
  scheduleLast30dHistoryBackfill,
  scheduleLatestLast30dRefresh,
  scheduleNpmIngest,
} from '../npm/schedule'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await scheduleNpmIngest()
  await scheduleDailyDownloadsBackfill()
  await scheduleLatestLast30dRefresh()
  await scheduleLast30dHistoryBackfill()
  await svc.start()
})
