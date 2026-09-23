import { safeWrap } from '../../middlewares/errorMiddleware'

export default (app) => {
  // Insights projects routes
  app.post(
    '/collections/insights-projects/query',
    safeWrap(require('./insightsProjects/insightsProjectsQuery').default),
  )
  app.post(
    '/collections/insights-projects',
    safeWrap(require('./insightsProjects/insightsProjectsCreate').default),
  )
  app.delete(
    '/collections/insights-projects/:id',
    safeWrap(require('./insightsProjects/insightsProjectsDestroy').default),
  )
  app.post(
    '/collections/insights-projects/:id',
    safeWrap(require('./insightsProjects/insightsProjectsUpdate').default),
  )
  app.get(
    '/collections/insights-projects/:id',
    safeWrap(require('./insightsProjects/insightsProjectsGet').default),
  )
  app.post(
    '/collections/insights-projects/:id/docs-override',
    safeWrap(require('./insightsProjects/insightsProjectsDocsOverrideCreate').default),
  )
  app.delete(
    '/collections/insights-projects/:id/docs-override',
    safeWrap(require('./insightsProjects/insightsProjectsDocsOverrideDelete').default),
  )

  // Collections routes
  app.post('/collections/query', safeWrap(require('./collectionsQuery').default))
  app.post('/collections', safeWrap(require('./collectionsCreate').default))
  app.get('/collections/:id', safeWrap(require('./collectionsGet').default))
  app.post('/collections/:id', safeWrap(require('./collectionsUpdate').default))
  app.delete('/collections/:id', safeWrap(require('./collectionsDestroy').default))

  app.get('/segments/:id/repositories', safeWrap(require('./segmentsRepositoriesGet').default))
  app.get('/segments/:id/github-insights', safeWrap(require('./segmentsGithubInsightsGet').default))
  app.get('/segments/:id/widgets', safeWrap(require('./segmentsWidgetsGet').default))
}
