import {
  scheduleDailyDownloadsBackfill,
  scheduleLast30dHistoryBackfill,
  scheduleLatestLast30dRefresh,
  scheduleNpmIngest,
} from '../npm/schedule'
import { scheduleOsvSync } from '../osv/schedule'
import { scheduleMavenCritical, scheduleMavenNonCritical } from '../maven/schedule'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await scheduleNpmIngest()
  await scheduleDailyDownloadsBackfill()
  await scheduleLatestLast30dRefresh()
  await scheduleLast30dHistoryBackfill()
  await scheduleOsvSync()
  await scheduleMavenCritical()
  await scheduleMavenNonCritical()
  await svc.start()
})
