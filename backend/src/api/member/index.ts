import { safeWrap } from '../../middlewares/errorMiddleware'

export default (app) => {
  app.post(`/member/query`, safeWrap(require('./memberQuery').default))

  app.post(`/member/export`, safeWrap(require('./memberExport').default))

  app.post(`/member`, safeWrap(require('./memberCreate').default))
  app.put(`/member/:id`, safeWrap(require('./memberUpdate').default))
  app.delete(`/member`, safeWrap(require('./memberDestroy').default))
  app.post(`/member/autocomplete`, safeWrap(require('./memberAutocomplete').default))
  app.get(`/member/bot-suggestions`, safeWrap(require('./memberBotSuggestionsList').default))

  app.get(`/member/:id`, safeWrap(require('./memberFind').default))
  app.get(`/member/github/:id`, safeWrap(require('./memberFindGithub').default))
  app.put(`/member/:memberId/merge`, safeWrap(require('./memberMerge').default))

  app.get(`/member/:memberId/can-revert-merge`, safeWrap(require('./memberCanRevertMerge').default))

  app.post(`/member/:memberId/unmerge/preview`, safeWrap(require('./memberUnmergePreview').default))
  app.post(`/member/:memberId/unmerge`, safeWrap(require('./memberUnmerge').default))

  app.put(`/member/:memberId/no-merge`, safeWrap(require('./memberNotMerge').default))
  app.patch(`/member`, safeWrap(require('./memberUpdateBulk').default))

  require('./identity').default(app)
  require('./organization').default(app)
  require('./attributes').default(app)
  require('./affiliation').default(app)

  app.post(`/member/:id/data-issue`, safeWrap(require('./memberDataIssueCreate').default))
}
