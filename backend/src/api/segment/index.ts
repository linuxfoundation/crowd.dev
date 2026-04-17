import { safeWrap } from '../../middlewares/errorMiddleware'
import { segmentByIdMiddleware } from '../../middlewares/segmentMiddleware'

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

  // get/update segment by id — segmentByIdMiddleware overrides currentSegments with the target
  // segment so that permission checks validate against the actual resource being accessed
  app.get(`/segment/:segmentId`, segmentByIdMiddleware, safeWrap(require('./segmentFind').default))
  app.put(`/segment/:segmentId`, segmentByIdMiddleware, safeWrap(require('./segmentUpdate').default))
  // Multiple ids
  app.post(`/segment/id`, safeWrap(require('./segmentByIds').default))
}
