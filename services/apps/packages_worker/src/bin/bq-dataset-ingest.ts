import { scheduleOsspckgsBootstrap } from '../deps-dev/schedules/bootstrap'
import {
  schedulePypiDownloads30d,
  schedulePypiDownloadsDaily,
} from '../deps-dev/schedules/pypiDownloads'
import { scheduleOsspckgsCleanup } from '../schedules/cleanup'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await scheduleOsspckgsBootstrap()
  await scheduleOsspckgsCleanup()
  await schedulePypiDownloads30d()
  await schedulePypiDownloadsDaily()
  await svc.start()
})
