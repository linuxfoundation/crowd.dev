import { safeWrap } from '../../middlewares/errorMiddleware'

export default (app) => {
  app.post('/project-catalog', safeWrap(require('./projectCatalogUpsert').default))
}
