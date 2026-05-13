import passport from 'passport'

import { DEFAULT_TENANT_ID } from '@crowd/common'
import { RedisCache } from '@crowd/redis'

import { API_CONFIG, SLACK_CONFIG, TWITTER_CONFIG } from '../../conf'
import SegmentRepository from '../../database/repositories/segmentRepository'
import { authMiddleware } from '../../middlewares/authMiddleware'
import { safeWrap } from '../../middlewares/errorMiddleware'
import TenantService from '../../services/tenantService'

const decodeBase64Url = (data) => {
  data = data.replaceAll('-', '+').replaceAll('_', '/')
  while (data.length % 4) {
    data += '='
  }
  return atob(data)
}

export default (app) => {
  app.post(`/integration/query`, safeWrap(require('./integrationQuery').default))
  app.post(`/integration`, safeWrap(require('./integrationCreate').default))
  app.put(`/integration/:id`, safeWrap(require('./integrationUpdate').default))
  app.delete(`/integration`, safeWrap(require('./integrationDestroy').default))
  app.get(`/integration/autocomplete`, safeWrap(require('./integrationAutocomplete').default))
  app.get(`/integration/global`, safeWrap(require('./integrationGlobal').default))
  app.get(`/integration/global/status`, safeWrap(require('./integrationGlobalStatus').default))

  app.get(
    '/integration/github-installations',
    safeWrap(require('./helpers/githubGetInstallations').default),
  )

  app.post(
    '/integration/github-connect-installation',
    safeWrap(require('./helpers/githubConnectInstallation').default),
  )

  app.get(`/integration`, safeWrap(require('./integrationList').default))
  app.get(`/integration/:id`, safeWrap(require('./integrationFind').default))

  // Unified endpoint for all code platform integrations (github, gitlab, git, gerrit)
  app.get(
    `/integration/:id/repositories`,
    safeWrap(require('./helpers/getIntegrationRepositories').default),
  )

  app.put(`/authenticate/:code`, safeWrap(require('./helpers/githubAuthenticate').default))
  app.put(`/integration/:id/github/repos`, safeWrap(require('./helpers/githubMapRepos').default))

  app.get(
    `/integration/github/search/orgs`,
    safeWrap(require('./helpers/githubSearchOrgs').default),
  )
  app.get(
    `/integration/github/search/repos`,
    safeWrap(require('./helpers/githubSearchRepos').default),
  )
  app.get(
    `/integration/github/orgs/:org/repos`,
    safeWrap(require('./helpers/githubOrgRepos').default),
  )
  app.post('/github-nango-connect', safeWrap(require('./helpers/githubNangoConnect').default))
  app.put(
    `/discord-authenticate/:guild_id`,
    safeWrap(require('./helpers/discordAuthenticate').default),
  )
  app.put(`/reddit-onboard`, safeWrap(require('./helpers/redditOnboard').default))
  app.put('/linkedin-connect', safeWrap(require('./helpers/linkedinConnect').default))
  app.post('/linkedin-onboard', safeWrap(require('./helpers/linkedinOnboard').default))

  app.post(`/integration/progress/list`, safeWrap(require('./integrationProgressList').default))

  app.get(`/integration/progress/:id`, safeWrap(require('./integrationProgress').default))

  app.get(`/integration/mapped-repos/:id`, safeWrap(require('./integrationMappedRepos').default))

  // Git
  app.put(`/git-connect`, safeWrap(require('./helpers/gitAuthenticate').default))
  app.put(`/confluence-connect`, safeWrap(require('./helpers/confluenceAuthenticate').default))
  app.put(`/gerrit-connect`, safeWrap(require('./helpers/gerritAuthenticate').default))
  app.get('/devto-validate', safeWrap(require('./helpers/devtoValidators').default))
  app.get('/reddit-validate', safeWrap(require('./helpers/redditValidator').default))
  app.post('/devto-connect', safeWrap(require('./helpers/devtoCreateOrUpdate').default))
  app.post('/hackernews-connect', safeWrap(require('./helpers/hackerNewsCreateOrUpdate').default))

  app.post(
    '/stackoverflow-connect',
    safeWrap(require('./helpers/stackOverflowCreateOrUpdate').default),
  )
  app.get('/stackoverflow-validate', safeWrap(require('./helpers/stackOverflowValidator').default))
  app.get('/stackoverflow-volume', safeWrap(require('./helpers/stackOverflowVolume').default))

  app.post('/discourse-connect', safeWrap(require('./helpers/discourseCreateOrUpdate').default))

  app.post('/discourse-validate', safeWrap(require('./helpers/discourseValidator').default))

  app.post('/discourse-test-webhook', safeWrap(require('./helpers/discourseTestWebhook').default))

  app.post('/groupsio-connect', safeWrap(require('./helpers/groupsioConnectOrUpdate').default))

  app.post('/groupsio-get-token', safeWrap(require('./helpers/groupsioGetToken').default))

  app.post('/groupsio-verify-group', safeWrap(require('./helpers/groupsioVerifyGroup').default))

  app.post(
    '/groupsio-get-user-subscriptions',
    safeWrap(require('./helpers/groupsioGetUserSubscriptions').default),
  )

  app.post('/jira-connect', safeWrap(require('./helpers/jiraConnectOrUpdate').default))

  app.get('/gitlab/connect', safeWrap(require('./helpers/gitlabAuthenticate').default))

  app.get('/gitlab/callback', safeWrap(require('./helpers/gitlabAuthenticateCallback').default))

  app.put(`/integration/:id/gitlab/repos`, safeWrap(require('./helpers/gitlabMapRepos').default))

  if (TWITTER_CONFIG.clientId) {
    /**
     * Using the passport.authenticate this endpoint forces a
     * redirect to happen to the twitter oauth2 page.
     * We keep a state of the important variables such as tenantId, redirectUrl, ..
     * so that after user logs in through the twitter page, these
     * variables are forwarded back to the callback as well
     * This state is sent using the authenticator options and
     * manipulated through twitterStrategy.staticPKCEStore
     */
    app.get('/twitter/connect', safeWrap(require('./helpers/twitterAuthenticate').default), () => {
      // The request will be redirected for authentication, so this
      // function will not be called.
    })

    /**
     * OAuth2 callback endpoint.  After user successfully
     * logs in through twitter page s/he is redirected to
     * this endpoint. Few middlewares first mimic a proper
     * api request in this order:
     *
     * Set headers-> Auth middleware (currentUser)-> Set currentTenant
     * -> finally we call the project service to update the
     * corresponding project.
     *
     * We have to call these standart middlewares explicitly because
     * the request method is get and tenant id does not exist in the
     * uri as request parameters.
     *
     */
    app.get(
      '/twitter/callback',
      // passport.authenticate('twitter', {
      //   session: false,
      //   failureRedirect: `${API_CONFIG.frontendUrl}/integrations?error=true`,
      // }),
      async (req, _res, next) => {
        const stateQueryParam = req.query.state
        const decodedState = decodeBase64Url(stateQueryParam)
        req.state = JSON.parse(decodedState)
        next()
      },
      (req, _res, next) => {
        const { crowdToken } = req.state
        req.headers.authorization = `Bearer ${crowdToken}`
        next()
      },
      authMiddleware,
      async (req, _res, next) => {
        const tenantId = DEFAULT_TENANT_ID
        req.currentTenant = await new TenantService(req).findById(tenantId)
        next()
      },
      async (req, _res, next) => {
        const cache = new RedisCache('twitterPKCE', req.redis, req.log)
        const state = await cache.get(req.currentUser.id)
        const { segmentIds } = JSON.parse(state)
        const segmentRepository = new SegmentRepository(req)
        req.currentSegments = await segmentRepository.findInIds(segmentIds)
        next()
      },
      safeWrap(require('./helpers/twitterAuthenticateCallback').default),
    )
  }

  /**
   * Slack integration endpoints
   * These should be super similar to Twitter's, since we're also using passport.js
   */
  if (SLACK_CONFIG.clientId) {
    // path to start the OAuth flow
    app.get('/slack/connect', safeWrap(require('./helpers/slackAuthenticate').default))

    // OAuth callback url
    app.get(
      '/slack/callback',
      passport.authorize('slack', {
        session: false,
        failureRedirect: `${API_CONFIG.frontendUrl}/integrations?error=true`,
      }),
      async (req, _res, next) => {
        req.state = JSON.parse(Buffer.from(req.query.state, 'base64').toString())
        next()
      },
      (req, _res, next) => {
        const { crowdToken } = req.state
        req.headers.authorization = `Bearer ${crowdToken}`
        next()
      },
      authMiddleware,
      async (req, _res, next) => {
        const tenantId = DEFAULT_TENANT_ID
        req.currentTenant = await new TenantService(req).findById(tenantId)
        next()
      },
      async (req, _res, next) => {
        const { segmentIds } = req.state
        const segmentRepository = new SegmentRepository(req)
        req.currentSegments = await segmentRepository.findInIds(segmentIds)
        next()
      },
      safeWrap(require('./helpers/slackAuthenticateCallback').default),
    )
  }
}
