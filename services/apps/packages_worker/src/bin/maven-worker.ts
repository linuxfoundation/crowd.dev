import { scheduleMavenIngestion } from '../maven/schedule'
import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await scheduleMavenIngestion()
  await svc.start()
})
