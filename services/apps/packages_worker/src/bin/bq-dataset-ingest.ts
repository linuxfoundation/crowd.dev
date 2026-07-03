import { scheduleOsspckgsBootstrap } from '../deps-dev/schedules/bootstrap'
import {
  schedulePypiDownloads30d,
  schedulePypiDownloadsDaily,
} from '../pypi/downloads/pypiDownloads'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await scheduleOsspckgsBootstrap()
  await schedulePypiDownloads30d()
  await schedulePypiDownloadsDaily()
  await svc.start()
})
