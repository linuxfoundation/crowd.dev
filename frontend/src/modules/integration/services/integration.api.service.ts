// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import authAxios from '@/shared/axios/auth-axios';
import { getRouter } from '@/router/instance';
import type { IntegrationMapping } from '@/modules/admin/modules/integration/types/Integration';
import type { IntegrationProgress } from '@/modules/integration/types/IntegrationProgress';
import type {
  DiscordConnectPayload,
  GithubConnectPayload,
  GitlabConnectPayload,
} from '@/modules/integration/types/ConnectPayloads';
import type {
  GithubInstallation,
  IntegrationResponse,
  IntegrationWithSegment,
  MappedRepos,
} from '@/modules/integration/types/IntegrationResponse';
import type { GitHubSettings } from '@/config/integrations/github-nango/types/GithubSettings';

type SegmentIds = (string | null | undefined)[];

const getSegments = () => ({ segments: [getRouter().currentRoute.value.params.id] });

const update = async (
  id: string,
  data: Record<string, unknown>,
  segments: string[] = [],
): Promise<IntegrationResponse> => {
  const body: Record<string, unknown> = {
    ...data,
    ...getSegments(),
  };

  body.segments = segments.length ? segments : body.segments;

  const response = await authAxios.put(`/integration/${id}`, body);

  return response.data;
};

const destroyAll = async (ids: string[]): Promise<unknown> => {
  const params = {
    ids,
    ...getSegments(),
  };

  const response = await authAxios.delete('/integration', {
    params,
  });

  return response.data;
};

const create = async (
  data: Record<string, unknown>,
  segments: string[] = [],
): Promise<IntegrationResponse> => {
  const body: Record<string, unknown> = {
    ...data,
    ...getSegments(),
  };

  body.segments = segments.length ? segments : body.segments;

  const response = await authAxios.post('/integration', body);

  return response.data;
};

const find = async (id: string): Promise<IntegrationWithSegment> => {
  const response = await authAxios.get(`/integration/${id}`, {
    params: getSegments(),
  });

  return response.data;
};

const list = async (
  filter: Record<string, unknown> | null,
  orderBy: string | null,
  limit: number | null,
  offset: number | null,
  segments: SegmentIds,
): Promise<{ rows: IntegrationResponse[]; count: number }> => {
  const body = {
    filter,
    orderBy,
    limit,
    offset,
    ...(segments.length ? { segments } : getSegments()),
  };

  const response = await authAxios.post('/integration/query', body);

  return response.data;
};

const devtoConnect = async (
  users: string[],
  organizations: string[],
  apiKey: string,
  segments: SegmentIds,
): Promise<IntegrationResponse> => {
  const response = await authAxios.post('/devto-connect', {
    users,
    organizations,
    apiKey,
    ...getSegments(),
    segments,
  });

  return response.data;
};

const devtoValidateAPIKey = async (apiKey: string): Promise<unknown> => {
  const response = await authAxios.get('/devto-validate', {
    params: {
      apiKey,
      ...getSegments(),
    },
  });

  return response.data;
};

const hackerNewsConnect = async (
  keywords: string[],
  urls: string[],
  segments: SegmentIds,
): Promise<IntegrationResponse> => {
  const response = await authAxios.post('/hackernews-connect', {
    keywords,
    urls,
    ...getSegments(),
    segments,
  });

  return response.data;
};

const githubNangoConnect = async (
  settings: GitHubSettings,
  segmentIds: SegmentIds,
  mapping: Record<string, string>,
  integrationId?: string,
): Promise<IntegrationResponse> => {
  const body: Record<string, unknown> = {
    integrationId,
    settings,
    ...getSegments(),
    mapping,
  };

  body.segments = segmentIds.length ? segmentIds : body.segments;

  const response = await authAxios.post('/github-nango-connect', body);

  return response.data;
};

const githubConnect = async (
  code: GithubConnectPayload['code'],
  installId: GithubConnectPayload['installId'],
  setupAction: GithubConnectPayload['setupAction'],
): Promise<IntegrationResponse> => {
  const body = {
    installId,
    setupAction,
    ...getSegments(),
  };
  const response = await authAxios.put(`/authenticate/${code}`, body);
  return response.data;
};

const githubMapRepos = async (
  integrationId: string,
  mapping: Record<string, string>,
  segments: SegmentIds,
  isUpdateTransaction = false,
): Promise<unknown> => {
  const response = await authAxios.put(`/integration/${integrationId}/github/repos`, {
    mapping,
    segments,
    isUpdateTransaction,
  });
  return response.data;
};

const fetchIntegrationRepositories = async (integration: {
  id: string;
  segmentId?: string | null;
}): Promise<IntegrationMapping[]> => {
  const response = await authAxios.get(`/integration/${integration.id}/repositories`, {
    params: {
      segments: [integration.segmentId],
    },
  });
  return response.data;
};

const fetchGitHubMappings = async (
  integration: Parameters<typeof fetchIntegrationRepositories>[0],
): Promise<IntegrationMapping[]> => fetchIntegrationRepositories(integration);

const fetchGitLabMappings = async (
  integration: Parameters<typeof fetchIntegrationRepositories>[0],
): Promise<IntegrationMapping[]> => fetchIntegrationRepositories(integration);

const fetchGitMappings = async (
  integration: Parameters<typeof fetchIntegrationRepositories>[0],
): Promise<IntegrationMapping[]> => fetchIntegrationRepositories(integration);

const redditOnboard = async (
  subreddits: string[],
  segmentId: string | null,
): Promise<IntegrationResponse> => {
  const body = {
    subreddits,
    segments: [segmentId],
  };
  const response = await authAxios.put('/reddit-onboard', body);
  return response.data;
};

const linkedinConnect = async (segmentId: string | null): Promise<IntegrationResponse> => {
  const response = await authAxios.put('/linkedin-connect', {
    segments: [segmentId],
  });

  return response.data;
};

const linkedinOnboard = async (
  organizationId: string | null,
  segmentId: string | null,
): Promise<IntegrationResponse> => {
  const body = {
    organizationId,
    segments: [segmentId],
  };

  const response = await authAxios.post('/linkedin-onboard', body);

  return response.data;
};

const discordConnect = async (
  guildId: DiscordConnectPayload['guildId'],
): Promise<IntegrationResponse> => {
  const response = await authAxios.put(`/discord-authenticate/${guildId}`, getSegments());
  return response.data;
};

const devtoValidateUser = async (username: string, apiKey?: string): Promise<unknown> => {
  const response = await authAxios.get('/devto-validate', {
    params: {
      username,
      apiKey,
      ...getSegments(),
    },
  });

  return response.data;
};

const devtoValidateOrganization = async (
  organization: string,
  apiKey?: string,
): Promise<unknown> => {
  const response = await authAxios.get('/devto-validate', {
    params: {
      organization,
      apiKey,
      ...getSegments(),
    },
  });

  return response.data;
};

const redditValidate = async (subreddit: string): Promise<unknown> => {
  const response = await authAxios.get('/reddit-validate', {
    params: {
      subreddit,
      ...getSegments(),
    },
  });

  return response.data;
};

const stackOverflowValidate = async (tag: string): Promise<{ items: { count: number }[] }> => {
  const response = await authAxios.get('/stackoverflow-validate', {
    params: {
      tag,
      ...getSegments(),
    },
  });

  return response.data;
};

const stackOverflowVolume = async (keywords: string): Promise<number> => {
  const response = await authAxios.get('/stackoverflow-volume', {
    params: {
      keywords,
      ...getSegments(),
    },
  });

  return response.data.total;
};

const stackOverflowOnboard = async (
  segmentId: string | null,
  tags: string[],
  keywords: string[],
): Promise<IntegrationResponse> => {
  const response = await authAxios.post('/stackoverflow-connect', {
    tags,
    keywords,
    segments: [segmentId],
  });

  return response.data;
};

const hubspotConnect = async (): Promise<unknown> => {
  const response = await authAxios.post('/hubspot-connect', {});

  return response.data;
};

const gitConnect = async (
  remotes: string[],
  segments: SegmentIds = [],
): Promise<IntegrationResponse> => {
  const response = await authAxios.put('/git-connect', {
    remotes,
    ...getSegments(),
    segments,
  });

  return response.data;
};

const confluenceConnect = async (
  id: string | undefined,
  settings: Record<string, unknown>,
  segmentId: string | null,
): Promise<IntegrationResponse> => {
  const response = await authAxios.put('/confluence-connect', {
    id,
    settings,
    segments: [segmentId],
  });

  return response.data;
};

const gerritConnect = async (
  remote: { orgURL: string; repoNames: string[]; enableAllRepos: boolean; enableGit: boolean },
  segments: SegmentIds = [],
): Promise<IntegrationResponse> => {
  const response = await authAxios.put('/gerrit-connect', {
    remote,
    ...getSegments(),
    segments,
  });

  return response.data;
};

const discourseValidateAPI = async (forumHostname: string, apiKey: string): Promise<boolean> => {
  const response = await authAxios.post('/discourse-validate', {
    forumHostname,
    apiKey,
    apiUsername: 'system',
    ...getSegments(),
  });

  return response.status === 200;
};

const discourseConnect = async (
  forumHostname: string,
  apiKey: string,
  webhookSecret: string | undefined,
  segments: SegmentIds = [],
): Promise<IntegrationResponse> => {
  const response = await authAxios.post('/discourse-connect', {
    forumHostname,
    apiKey,
    apiUsername: 'system',
    webhookSecret,
    ...getSegments(),
    segments,
  });

  return response.data;
};

const discourseSoftConnect = async (
  forumHostname: string,
  apiKey: string,
  webhookSecret: string,
): Promise<string> => {
  const response = await authAxios.post('/discourse-soft-connect', {
    forumHostname,
    apiKey,
    apiUsername: 'system',
    webhookSecret,
  });

  return response.data.id;
};

const discourseVerifyWebhook = async (integrationId: string | undefined): Promise<boolean> => {
  const response = await authAxios.post('/discourse-test-webhook', {
    integrationId,
  });

  return response.data.isWebhooksReceived;
};

const groupsioConnect = async (
  email: string,
  token: string,
  tokenExpiry: string,
  password: string,
  groups: unknown[],
  autoImports: unknown[],
  segments: SegmentIds = [],
): Promise<IntegrationResponse> => {
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
};

const groupsioGetToken = async (
  email: string,
  password: string,
  twoFactorCode: string | null = null,
  segments: SegmentIds = [],
): Promise<{ groupsioCookie: string; groupsioCookieExpiry: string }> => {
  const response = await authAxios.post('/groupsio-get-token', {
    email,
    password,
    twoFactorCode,
    ...getSegments(),
    segments,
  });

  return response.data;
};

const groupsioVerifyGroup = async (groupName: string, cookie: string): Promise<unknown> => {
  const response = await authAxios.post('/groupsio-verify-group', {
    groupName,
    cookie,
    ...getSegments(),
  });

  return response.data;
};

const groupsioGetUserSubscriptions = async (cookie: string): Promise<unknown> => {
  const response = await authAxios.post('/groupsio-get-user-subscriptions', {
    cookie,
    ...getSegments(),
  });

  return response.data;
};

const jiraConnect = async (
  id: string | undefined,
  url: string,
  username: string,
  personalAccessToken: string,
  apiToken: string,
  projects: string[],
  segments: SegmentIds = [],
): Promise<IntegrationResponse> => {
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
};

const fetchIntegrationsProgress = async (segments?: string[]): Promise<IntegrationProgress[]> => {
  const response = await authAxios.post('/integration/progress/list', {
    segments: segments || undefined,
  });

  return response.data;
};

const fetchGitHubMappedRepos = async (segmentId: string): Promise<MappedRepos | null> => {
  const response = await authAxios.get(`/integration/mapped-repos/${segmentId}`, {
    params: getSegments(),
  });

  return response.data;
};

const githubConnectInstallation = async (installId: string): Promise<unknown> => {
  const response = await authAxios.post('/integration/github-connect-installation', {
    installId,
    ...getSegments(),
  });

  return response.data;
};

const getGithubInstallations = async (): Promise<GithubInstallation[]> => {
  const response = await authAxios.get('/integration/github-installations', {
    params: getSegments(),
  });

  return response.data;
};

const gitlabConnect = async (
  code: GitlabConnectPayload['code'],
  state: GitlabConnectPayload['state'],
  segments: SegmentIds = [],
): Promise<IntegrationResponse> => {
  const response = await authAxios.get('/gitlab/callback', {
    params: {
      code,
      state,
      ...getSegments(),
      segments,
    },
  });
  return response.data;
};

const mapGitlabRepos = async (
  integrationId: string,
  mapping: Record<string, string>,
  projectIds: number[],
  segments: SegmentIds,
): Promise<unknown> => {
  const response = await authAxios.put(`/integration/${integrationId}/gitlab/repos`, {
    mapping,
    projectIds,
    segments,
  });
  return response.data;
};

export const IntegrationService = {
  update,
  destroyAll,
  create,
  find,
  list,
  devtoConnect,
  devtoValidateAPIKey,
  hackerNewsConnect,
  githubNangoConnect,
  githubConnect,
  githubMapRepos,
  fetchIntegrationRepositories,
  fetchGitHubMappings,
  fetchGitLabMappings,
  fetchGitMappings,
  redditOnboard,
  linkedinConnect,
  linkedinOnboard,
  discordConnect,
  devtoValidateUser,
  devtoValidateOrganization,
  redditValidate,
  stackOverflowValidate,
  stackOverflowVolume,
  stackOverflowOnboard,
  hubspotConnect,
  gitConnect,
  confluenceConnect,
  gerritConnect,
  discourseValidateAPI,
  discourseConnect,
  discourseSoftConnect,
  discourseVerifyWebhook,
  groupsioConnect,
  groupsioGetToken,
  groupsioVerifyGroup,
  groupsioGetUserSubscriptions,
  jiraConnect,
  fetchIntegrationsProgress,
  fetchGitHubMappedRepos,
  githubConnectInstallation,
  getGithubInstallations,
  gitlabConnect,
  mapGitlabRepos,
};
