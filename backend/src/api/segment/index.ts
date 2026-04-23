import { safeWrap } from '../../middlewares/errorMiddleware'

export default (app) => {
  app.post(`/segment/projectGroup`, safeWrap(require('./segmentCreateProjectGroup').default))
  app.post(`/segment/project`, safeWrap(require('./segmentCreateProject').default))
  app.post(`/segment/subproject`, safeWrap(require('./segmentCreateSubproject').default))

  // query all project groups
  app.post(`/segment/projectGroup/query`, safeWrap(require('./segmentProjectGroupQuery').default))

  // query all projects
  app.post(`/segment/project/query`, safeWrap(require('./segmentProjectQuery').default))

  // query all subprojects
  app.post(`/segment/subproject/query`, safeWrap(require('./segmentSubprojectQuery').default))

  // query all subprojects lite
  app.post(
    `/segment/subproject/query-lite`,
    safeWrap(require('./segmentSubprojectQueryLite').default),
  )

  // get segment by id
  app.get(`/segment/:segmentId`, safeWrap(require('./segmentFind').default))
  app.put(`/segment/:segmentId`, safeWrap(require('./segmentUpdate').default))
  // Multiple ids
  app.post(`/segment/id`, safeWrap(require('./segmentByIds').default))
}
