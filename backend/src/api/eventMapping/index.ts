import { safeWrap } from '@/middlewares/errorMiddleware'

export default (app) => {
  app.get('/event-mapping', safeWrap(require('./eventMappingList').default))
  app.post('/event-mapping/confirm', safeWrap(require('./eventMappingConfirm').default))
  app.post('/event-mapping/ignore', safeWrap(require('./eventMappingIgnore').default))
  app.post('/event-mapping/restore', safeWrap(require('./eventMappingRestore').default))
}
