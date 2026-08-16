import authAxios from '@/shared/axios/auth-axios';
import { AuthService } from '@/modules/auth/services/auth.service';
import { router } from '@/router';

const getSegments = () => ({ segments: [router.currentRoute.value.params.id] });

export class IntegrationService {
  static async update(id, data, segments = []) {
    const body = {
      ...data,
      ...getSegments(),
    };

    body.segments = segments.length ? segments : body.segments;

    const response = await authAxios.put(`/integration/${id}`, body);

    return response.data;
  }

  static async destroyAll(ids) {
    const params = {
      ids,
      ...getSegments(),
    };

    const response = await authAxios.delete('/integration', {
      params,
    });

    return response.data;
  }

  static async create(data, segments = []) {
    const body = {
      ...data,
      ...getSegments(),
    };

    body.segments = segments.length ? segments : body.segments;

    const response = await authAxios.post('/integration', body);

    return response.data;
  }

  static async find(id) {
    const response = await authAxios.get(`/integration/${id}`, {
      params: getSegments(),
    });

    return response.data;
  }

  static async list(filter, orderBy, limit, offset, segments) {
    const body = {
      filter,
      orderBy,
      limit,
      offset,
      ...(segments.length ? { segments } : getSegments()),
    };

    const response = await authAxios.post('/integration/query', body);

    return response.data;
  }

  static async devtoConnect(users, organizations, apiKey, segments) {
    // Calling connect devto function in the backend.
    const response = await authAxios.post('/devto-connect', {
      users,
      organizations,
      apiKey,
      ...getSegments(),
      segments,
    });

    return response.data;
  }

  static async devtoValidateAPIKey(apiKey) {
    const response = await authAxios.get('/devto-validate', {
      params: {
        apiKey,
        ...getSegments(),
      },
    });

    return response.data;
  }

  static async hackerNewsConnect(keywords, urls, segments) {
    // Calling connect devto function in the backend.
    const response = await authAxios.post('/hackernews-connect', {
      keywords,
      urls,
      ...getSegments(),
      segments,
    });

    return response.data;
  }

  static async githubNangoConnect(
    settings,
    segmentIds,
    mapping,
    integrationId,
  ) {
    const body = {
      integrationId,
      settings,
      ...getSegments(),
      mapping,
    };

    body.segments = segmentIds.length ? segmentIds : body.segments;

    const response = await authAxios.post('/github-nango-connect', body);

    return response.data;
  }

  static async githubConnect(code, installId, setupAction) {
    // Ask backend to connect to GitHub through Oauth.
    // Install_id is the GitHub app installation id.
    const body = {
      installId,
      setupAction,
      ...getSegments(),
    };
    // Calling the authenticate function in the backend.
    const response = await authAxios.put(`/authenticate/${code}`, body);
    return response.data;
  }

  static async githubMapRepos(
    integrationId,
    mapping,
    segments,
    isUpdateTransaction = false,
  ) {
    const response = await authAxios.put(
      `/integration/${integrationId}/github/repos`,
      {
        mapping,
        segments,
        isUpdateTransaction,
      },
    );
    return response.data;
  }

  /**
   * Unified method to fetch repository mappings for any code platform integration
   * @param {Object} integration - The integration object with id and segmentId
   * @returns {Promise<Array>} Array of repository mappings
   */
  static async fetchIntegrationRepositories(integration) {
    const response = await authAxios.get(
      `/integration/${integration.id}/repositories`,
      {
        params: {
          segments: [integration.segmentId],
        },
      },
    );
    return response.data;
  }

  static async fetchGitHubMappings(integration) {
    return this.fetchIntegrationRepositories(integration);
  }

  static async fetchGitLabMappings(integration) {
    return this.fetchIntegrationRepositories(integration);
  }

  static async fetchGitMappings(integration) {
    return this.fetchIntegrationRepositories(integration);
  }

  static async redditOnboard(subreddits, segmentId) {
    // Ask backend to connect to GitHub through Oauth.
    // Install_id is the GitHub app installation id.
    const body = {
      subreddits,
      segments: [segmentId],
    };
    // Calling the authenticate function in the backend.
    const response = await authAxios.put('/reddit-onboard', body);
    return response.data;
  }

  static async linkedinConnect(segmentId) {
    const response = await authAxios.put('/linkedin-connect', {
      segments: [segmentId],
    });

    return response.data;
  }

  static async linkedinOnboard(organizationId, segmentId) {
    const body = {
      organizationId,
      segments: [segmentId],
    };

    const response = await authAxios.post('/linkedin-onboard', body);

    return response.data;
  }

  static async discordConnect(guildId) {
    // Calling the authenticate function in the backend.
    const response = await authAxios.put(
      `/discord-authenticate/${guildId}`,
      getSegments(),
    );
    return response.data;
  }

  static async devtoValidateUser(username, apiKey) {
    const response = await authAxios.get('/devto-validate', {
      params: {
        username,
        apiKey,
        ...getSegments(),
      },
    });

    return response.data;
  }

  static async devtoValidateOrganization(organization, apiKey) {
    const response = await authAxios.get('/devto-validate', {
      params: {
        organization,
        apiKey,
        ...getSegments(),
      },
    });

    return response.data;
  }

  static async redditValidate(subreddit) {
    const response = await authAxios.get('/reddit-validate', {
      params: {
        subreddit,
        ...getSegments(),
      },
    });

    return response.data;
  }

  static async stackOverflowValidate(tag) {
    const response = await authAxios.get('/stackoverflow-validate', {
      params: {
        tag,
        ...getSegments(),
      },
    });

    return response.data;
  }

  static async stackOverflowVolume(keywords) {
    const response = await authAxios.get('/stackoverflow-volume', {
      params: {
        keywords,
        ...getSegments(),
      },
    });

    return response.data.total;
  }

  static async stackOverflowOnboard(segmentId, tags, keywords) {
    // Calling the authenticate function in the backend.
    const response = await authAxios.post('/stackoverflow-connect', {
      tags,
      keywords,
      segments: [segmentId],
    });

    return response.data;
  }

  static async hubspotConnect() {
    // Calling the authenticate function in the backend.
    const response = await authAxios.post('/hubspot-connect', {});

    return response.data;
  }

  static async gitConnect(remotes, segments = []) {
    const response = await authAxios.put('/git-connect', {
      remotes,
      ...getSegments(),
      segments,
    });

    return response.data;
  }

  static async confluenceConnect(id, settings, segmentId) {
    const response = await authAxios.put('/confluence-connect', {
      id,
      settings,
      segments: [segmentId],
    });

    return response.data;
  }

  static async schedConnect(id, settings, segmentId) {
    const response = await authAxios.post('/sched-connect', {
      id,
      settings,
      segments: [segmentId],
    });

    return response.data;
  }

  static async gerritConnect(remote, segments = []) {
    const response = await authAxios.put('/gerrit-connect', {
      remote,
      ...getSegments(),
      segments,
    });

    return response.data;
  }

  static async discourseValidateAPI(forumHostname, apiKey) {
    const response = await authAxios.post('/discourse-validate', {
      forumHostname,
      apiKey,
      apiUsername: 'system',
      ...getSegments(),
    });

    return response.status === 200;
  }

  static async discourseConnect(
    forumHostname,
    apiKey,
    webhookSecret,
    segments = [],
  ) {
    const response = await authAxios.post('/discourse-connect', {
      forumHostname,
      apiKey,
      apiUsername: 'system',
      webhookSecret,
      ...getSegments(),
      segments,
    });

    return response.data;
  }

  static async discourseSoftConnect(forumHostname, apiKey, webhookSecret) {
    const response = await authAxios.post('/discourse-soft-connect', {
      forumHostname,
      apiKey,
      apiUsername: 'system',
      webhookSecret,
    });

    return response.data.id;
  }

  static async discourseVerifyWebhook(integrationId) {
    const response = await authAxios.post('/discourse-test-webhook', {
      integrationId,
    });

    return response.data.isWebhooksReceived;
  }

  static async groupsioConnect(
    email,
    token,
    tokenExpiry,
    password,
    groups,
    autoImports,
    segments = [],
  ) {
    const response = await authAxios.post('/groupsio-connect', {
      email,
      token,
      tokenExpiry,
      password,
      groups,
      autoImports,
      ...getSegments(),
      segments,
    });

    return response.data;
  }

  static async groupsioGetToken(
    email,
    password,
    twoFactorCode = null,
    segments = [],
  ) {
    const response = await authAxios.post('/groupsio-get-token', {
      email,
      password,
      twoFactorCode,
      ...getSegments(),
      segments,
    });

    return response.data;
  }

  static async groupsioVerifyGroup(groupName, cookie) {
    const response = await authAxios.post('/groupsio-verify-group', {
      groupName,
      cookie,
      ...getSegments(),
    });

    return response.data;
  }

  static async groupsioGetUserSubscriptions(cookie) {
    const response = await authAxios.post('/groupsio-get-user-subscriptions', {
      cookie,
      ...getSegments(),
    });

    return response.data;
  }

  static async jiraConnect(
    id,
    url,
    username,
    personalAccessToken,
    apiToken,
    projects,
    segments = [],
  ) {
    const response = await authAxios.post('/jira-connect', {
      id,
      url,
      username,
      personalAccessToken,
      apiToken,
      projects,
      ...getSegments(),
      segments,
    });

    return response.data;
  }

  static async fetchIntegrationsProgress(segments) {
    const response = await authAxios.post('/integration/progress/list', {
      segments: segments || undefined,
    });

    return response.data;
  }

  static async fetchGitHubMappedRepos(segmentId) {
    const response = await authAxios.get(`/integration/mapped-repos/${segmentId}`, {
      params: getSegments(),
    });

    return response.data;
  }

  static async githubConnectInstallation(installId) {
    const response = await authAxios.post(
      '/integration/github-connect-installation',
      {
        installId,
        ...getSegments(),
      },
    );

    return response.data;
  }

  static async getGithubInstallations() {
    const response = await authAxios.get('/integration/github-installations', {
      params: getSegments(),
    });

    return response.data;
  }

  static async gitlabConnect(code, state, segments = []) {
    const response = await authAxios.get('/gitlab/callback', {
      params: {
        code,
        state,
        ...getSegments(),
        segments,
      },
    });
    return response.data;
  }

  static async mapGitlabRepos(integrationId, mapping, projectIds, segments) {
    const response = await authAxios.put(
      `/integration/${integrationId}/gitlab/repos`,
      { mapping, projectIds, segments },
    );
    return response.data;
  }
}
