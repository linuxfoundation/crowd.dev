import { svc } from '../service'

setImmediate(async () => {
  await svc.init()
  await svc.start()
})
