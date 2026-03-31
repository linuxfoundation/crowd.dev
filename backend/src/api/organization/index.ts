import { safeWrap } from '../../middlewares/errorMiddleware'

export default (app) => {
  app.post(`/organization`, safeWrap(require('./organizationCreate').default))
  app.post(`/organization/query`, safeWrap(require('./organizationQuery').default))
  app.put(`/organization/:id`, safeWrap(require('./organizationUpdate').default))
  app.delete(`/organization`, safeWrap(require('./organizationDestroy').default))
  app.post(`/organization/autocomplete`, safeWrap(require('./organizationAutocomplete').default))
  app.get(`/organization/:id`, safeWrap(require('./organizationFind').default))

  app.put(`/organization/:organizationId/merge`, safeWrap(require('./organizationMerge').default))

  app.put(
    `/organization/:organizationId/no-merge`,
    safeWrap(require('./organizationNotMerge').default),
  )

  app.get(
    `/organization/:organizationId/can-revert-merge`,
    safeWrap(require('./organizationCanRevertMerge').default),
  )

  app.post(
    `/organization/:organizationId/unmerge/preview`,
    safeWrap(require('./organizationUnmergePreview').default),
  )

  app.post(
    `/organization/:organizationId/unmerge`,
    safeWrap(require('./organizationUnmerge').default),
  )

  app.post(`/organization/export`, safeWrap(require('./organizationExport').default))

  app.post(`/organization/id`, safeWrap(require('./organizationByIds').default))

  // list organizations across all segments
  app.post(`/organization/list`, safeWrap(require('./organizationList').default))

  app.post(
    `/organization/:id/data-issue`,
    safeWrap(require('./organizationDataIssueCreate').default),
  )
}
