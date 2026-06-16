import { safeWrap } from '../../middlewares/errorMiddleware'

export default (app) => {
  app.get('/ossprey/metrics', safeWrap(require('./metrics').default))
  // /packages/scatter must be registered before /packages to avoid Express treating 'scatter' as a path param
  app.get('/ossprey/packages/scatter', safeWrap(require('./packageScatter').default))
  app.get('/ossprey/packages', safeWrap(require('./packageList').default))
  app.get('/ossprey/activity', safeWrap(require('./activityFeed').default))
}
