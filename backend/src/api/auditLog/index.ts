import { safeWrap } from '../../middlewares/errorMiddleware'

export default (app) => {
  app.post(`/audit-logs/query`, safeWrap(require('./auditLogsQuery').default))
}
