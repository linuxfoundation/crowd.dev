import {
  scheduleRubyGemsCriticalIngestion,
  scheduleRubyGemsDependentsIngestion,
  scheduleRubyGemsIngestion,
} from '../rubygems/schedule'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await scheduleRubyGemsIngestion()
  await scheduleRubyGemsCriticalIngestion()
  await scheduleRubyGemsDependentsIngestion()
  await svc.start()
})
