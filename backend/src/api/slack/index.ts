import { SLACK_CONFIG } from '../../conf/index'
import { safeWrap } from '../../middlewares/errorMiddleware'

export default (app) => {
  if (
    SLACK_CONFIG.onboardingAppId &&
    SLACK_CONFIG.onboardingAppToken &&
    SLACK_CONFIG.onboardingTeamId
  ) {
    app.post('/slack/commands', safeWrap(require('./command').default))
  }
}
