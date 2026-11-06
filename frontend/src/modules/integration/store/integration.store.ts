// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import { defineStore } from 'pinia';
import { computed, ref } from 'vue';
import { IntegrationService } from '@/modules/integration/services/integration.api.service';
import Errors from '@/shared/error/errors';
import type { RouteParamsRaw, Router } from 'vue-router';
import { getRouter } from '@/router/instance';
import { isCurrentDateAfterGivenWorkingDays } from '@/utils/date';
import { lfIntegrations } from '@/config/integrations';
import type { IntegrationConfig } from '@/config/integrations';
import { ToastStore } from '@/shared/message/notification';
import { showIntegrationProgressNotification } from '@/modules/integration/helpers/integration-progress-notification';
import type { IntegrationRecord } from '@/modules/integration/types/IntegrationResponse';
import type { IntegrationApiError } from '@/modules/integration/types/IntegrationApiError';
import type { GitHubSettings } from '@/config/integrations/github-nango/types/GithubSettings';
import { githubRepoConnectionMatcher } from '@/config/integrations/github-nango/github-nango.helpers';
import type {
  ConfluenceConnectPayload,
  DevtoConnectPayload,
  DiscordConnectPayload,
  DiscourseConnectPayload,
  GerritConnectPayload,
  GitConnectPayload,
  GithubConnectPayload,
  GitlabConnectPayload,
  GroupsioConnectPayload,
  HackerNewsConnectPayload,
  JiraConnectPayload,
  LinkedinConnectPayload,
  LinkedinOnboardPayload,
  RedditOnboardPayload,
  StackOverflowOnboardPayload,
} from '@/modules/integration/types/ConnectPayloads';

// shortcut: segmentId is always set after connect, as before the migration. revisit: when IntegrationRecord.segmentId is required
const notifyProgress = (platform: string, segmentId?: string | null) =>
  showIntegrationProgressNotification(platform, segmentId as string);

const ERROR_BANNER_WORKING_DAYS_DISPLAY = 3;

const FIRST_ACTIVITIES_MESSAGE =
  'The first activities will show up in a couple of seconds. <br /> <br /> ' +
  'This process might take a few minutes to finish, depending on the amount of data.';

export type IntegrationListItem = IntegrationRecord & Partial<IntegrationConfig>;

const getNangoStatus = (integration: IntegrationRecord) => {
  const settings = integration.settings as unknown as GitHubSettings;
  const isConnected = githubRepoConnectionMatcher(settings);
  const allRepos = settings.orgs.flatMap((org) => org.repos ?? []);
  return allRepos.every((repo) => isConnected(repo.url)) ? 'done' : 'in-progress';
};

// Temporary workaround while two GitHub integrations are supported
const mapNangoIntegration = (row: IntegrationRecord): IntegrationRecord => {
  if (row.platform === 'github-nango') {
    return {
      ...row,
      platform: 'github',
      isNango: true,
      status: getNangoStatus(row),
    };
  }
  return row;
};

const withConfig = (integration: IntegrationRecord): IntegrationListItem => ({
  ...integration,
  ...lfIntegrations()[integration.platform],
});

export const useIntegrationStore = defineStore('integration', () => {
  const integrations = ref<Record<string, IntegrationRecord>>({});
  const allIds = ref<string[]>([]);
  const count = ref(0);
  const loading = ref(false);
  const loaded = ref(false);
  const segmentId = ref<string | null>(null);

  const loadingFetch = computed(() => loading.value);
  const loadingFind = computed(() => (id: string) => integrations.value[id]?.loading);
  const findById = computed(() => (id: string) => integrations.value[id]);

  const list = computed(() =>
    Object.keys(integrations.value).reduce<Record<string, IntegrationListItem>>((acc, key) => {
      acc[key] = withConfig(integrations.value[key]);
      return acc;
    }, {}),
  );

  const listByPlatform = computed(() =>
    Object.keys(integrations.value).reduce<Record<string, IntegrationListItem>>((acc, key) => {
      acc[integrations.value[key].platform] = withConfig(integrations.value[key]);
      return acc;
    }, {}),
  );

  const array = computed(() => allIds.value.map((id) => list.value[id]));
  const findByPlatform = computed(
    () => (platform: string) => array.value.find((w) => w.platform === platform),
  );
  const active = computed(() =>
    array.value.filter((i) => i.status === 'done' || i.status === 'in-progress'),
  );
  const activeList = computed(() =>
    active.value.reduce<Record<string, IntegrationListItem>>((acc, item) => {
      acc[item.platform] = item;
      return acc;
    }, {}),
  );
  const inProgress = computed(() => array.value.filter((i) => i.status === 'in-progress'));
  const withErrors = computed(() =>
    array.value.filter(
      (i) =>
        i.status === 'error' &&
        isCurrentDateAfterGivenWorkingDays(i.updatedAt!, ERROR_BANNER_WORKING_DAYS_DISPLAY),
    ),
  );
  const withNoData = computed(() => array.value.filter((i) => i.status === 'no-data'));
  const needsReconnect = computed(() => array.value.filter((i) => i.status === 'needs-reconnect'));
  const hasRows = computed(() => count.value > 0);

  const upsert = (record: IntegrationRecord) => {
    integrations.value[record.id] = record;
    if (!allIds.value.includes(record.id)) {
      allIds.value.push(record.id);
      count.value += 1;
    }
  };

  const fetch = async (segments: (string | null | undefined)[] = []): Promise<void> => {
    try {
      loading.value = true;

      const response = await IntegrationService.list(null, null, null, null, segments);

      const rows: IntegrationRecord[] = response.rows.map(mapNangoIntegration);

      const byId: Record<string, IntegrationRecord> = {};
      const ids: string[] = [];
      rows.forEach((integration) => {
        byId[integration.id] = integration;
        if (!ids.includes(integration.id)) {
          ids.push(integration.id);
        }
      });

      loading.value = false;
      segmentId.value =
        ((getRouter() as Router).currentRoute.value.params.id as string | undefined) ?? null;
      integrations.value = byId;
      allIds.value = ids;
      count.value = response.count;
      loaded.value = true;
    } catch (error) {
      Errors.handle(error);
      loading.value = false;
      count.value = 0;
    }
  };

  const find = async (id: string): Promise<void> => {
    try {
      if (integrations.value[id]) {
        integrations.value[id].loading = true;
      }
      const response: IntegrationRecord | null = await IntegrationService.find(id);
      if (!response) {
        return;
      }
      const record = mapNangoIntegration(response);
      if (!integrations.value[record.id]) {
        return;
      }
      integrations.value[record.id] = record;
      integrations.value[record.id].loading = false;
    } catch (error) {
      Errors.handle(error);
      if (integrations.value[id]) {
        integrations.value[id].loading = false;
      }
    }
  };

  const destroy = async (id: string): Promise<void> => {
    try {
      loading.value = true;

      await IntegrationService.destroyAll([id]);
      ToastStore.success('Integration was disconnected successfully');

      loading.value = false;
      delete integrations.value[id];
      allIds.value.splice(allIds.value.indexOf(id), 1);
      count.value -= 1;
      fetch();
    } catch (error) {
      Errors.handle(error);
      loading.value = false;
    }
  };

  const pushToIntegration = (params: RouteParamsRaw) =>
    (getRouter() as Router).push({ name: 'integration', params });

  const connectGithub = async ({
    code,
    installId,
    setupAction,
  }: GithubConnectPayload): Promise<void> => {
    try {
      const integration: IntegrationRecord = await IntegrationService.githubConnect(
        code,
        installId,
        setupAction,
      );
      fetch();
      pushToIntegration({ id: integration.segmentId });
      fetch([integration.segmentId]);
    } catch (error) {
      Errors.handle(error);
    }
  };

  const connectGitlab = async ({
    code,
    state,
    segmentId: targetSegmentId,
    grandparentId,
  }: GitlabConnectPayload): Promise<void> => {
    try {
      const integration: IntegrationRecord = await IntegrationService.gitlabConnect(code, state, [
        targetSegmentId,
      ]);
      upsert(integration);
      fetch();
      pushToIntegration({ id: integration.segmentId, grandparentId });
      fetch([integration.segmentId]);
    } catch (error) {
      Errors.handle(error);
    }
  };

  const connectGit = async ({
    remotes,
    isUpdate,
    segmentId: targetSegmentId,
    grandparentId,
    errorHandler,
  }: GitConnectPayload): Promise<void> => {
    try {
      const integration: IntegrationRecord = await IntegrationService.gitConnect(remotes, [
        targetSegmentId,
      ]);
      upsert(integration);
      ToastStore.success(FIRST_ACTIVITIES_MESSAGE, {
        title: `Git integration ${isUpdate ? 'updated' : 'created'} successfully`,
      });
      pushToIntegration({ id: targetSegmentId, grandparentId });
    } catch (error) {
      if (errorHandler) {
        errorHandler(error as IntegrationApiError);
      } else {
        Errors.handle(error);
      }
    }
  };

  const connectConfluence = async ({
    id,
    settings,
    isUpdate,
    segmentId: targetSegmentId,
    grandparentId,
  }: ConfluenceConnectPayload): Promise<void> => {
    try {
      const integration: IntegrationRecord = await IntegrationService.confluenceConnect(
        id,
        settings,
        targetSegmentId,
      );
      upsert(integration);
      ToastStore.success(FIRST_ACTIVITIES_MESSAGE, {
        title: `Confluence integration ${isUpdate ? 'updated' : 'created'} successfully`,
      });
      pushToIntegration({ id: targetSegmentId, grandparentId });
    } catch (error) {
      Errors.handle(error);
    }
  };

  const connectGerrit = async ({
    orgURL,
    repoNames,
    enableAllRepos,
    enableGit,
    isUpdate,
    segmentId: targetSegmentId,
    grandparentId,
    errorHandler,
  }: GerritConnectPayload): Promise<void> => {
    try {
      const integration: IntegrationRecord = await IntegrationService.gerritConnect(
        {
          orgURL,
          repoNames,
          enableAllRepos,
          enableGit,
        },
        [targetSegmentId],
      );
      upsert(integration);
      ToastStore.success(FIRST_ACTIVITIES_MESSAGE, {
        title: `Gerrit integration ${isUpdate ? 'updated' : 'created'} successfully`,
      });
      fetch([targetSegmentId]);
      pushToIntegration({ id: integration.segmentId, grandparentId });
    } catch (error) {
      if (errorHandler) {
        errorHandler(error as IntegrationApiError);
      } else {
        Errors.handle(error);
      }
    }
  };

  const connectDiscourse = async ({
    forumHostname,
    apiKey,
    webhookSecret,
    isUpdate,
    segmentId: targetSegmentId,
    grandparentId,
  }: DiscourseConnectPayload): Promise<void> => {
    try {
      const integration: IntegrationRecord = await IntegrationService.discourseConnect(
        forumHostname,
        apiKey,
        webhookSecret,
        [targetSegmentId],
      );
      upsert(integration);
      ToastStore.success(FIRST_ACTIVITIES_MESSAGE, {
        title: `Discourse integration ${isUpdate ? 'updated' : 'created'} successfully`,
      });
      pushToIntegration({ id: integration.segmentId, grandparentId });
    } catch (error) {
      Errors.handle(error);
    }
  };

  const connectGroupsio = async ({
    email,
    token,
    tokenExpiry,
    password,
    groups,
    isUpdate,
    autoImports,
    segmentId: targetSegmentId,
    grandparentId,
  }: GroupsioConnectPayload): Promise<void> => {
    try {
      const integration: IntegrationRecord = await IntegrationService.groupsioConnect(
        email,
        token,
        tokenExpiry,
        password,
        groups,
        autoImports,
        [targetSegmentId],
      );
      upsert(integration);
      ToastStore.success(FIRST_ACTIVITIES_MESSAGE, {
        title: `
              groups.io integration ${isUpdate ? 'updated' : 'created'} successfully`,
      });
      pushToIntegration({ id: integration.segmentId, grandparentId });
    } catch (error) {
      Errors.handle(error);
      ToastStore.error('Something went wrong. Please try again later.');
    }
  };

  const connectJira = async ({
    id,
    url,
    username,
    personalAccessToken,
    apiToken,
    projects,
    isUpdate,
    segmentId: targetSegmentId,
    grandparentId,
  }: JiraConnectPayload): Promise<void> => {
    try {
      const integration: IntegrationRecord = await IntegrationService.jiraConnect(
        id,
        url,
        username,
        personalAccessToken,
        apiToken,
        projects,
        [targetSegmentId],
      );
      upsert(integration);
      ToastStore.success(FIRST_ACTIVITIES_MESSAGE, {
        title: `
              Jira integration ${isUpdate ? 'updated' : 'created'} successfully`,
      });
      pushToIntegration({ id: integration.segmentId, grandparentId });
    } catch (error) {
      Errors.handle(error);
    }
  };

  const onboardReddit = async ({
    subreddits,
    segmentId: targetSegmentId,
    grandparentId,
  }: RedditOnboardPayload): Promise<void> => {
    try {
      const integration: IntegrationRecord = await IntegrationService.redditOnboard(
        subreddits,
        targetSegmentId,
      );
      upsert(integration);
      notifyProgress('reddit', integration.segmentId);
      pushToIntegration({ id: targetSegmentId, grandparentId });
    } catch (error) {
      Errors.handle(error);
    }
  };

  const connectLinkedin = async ({
    segmentId: targetSegmentId,
    grandparentId,
  }: LinkedinConnectPayload): Promise<void> => {
    try {
      const integration: IntegrationRecord =
        await IntegrationService.linkedinConnect(targetSegmentId);
      upsert(integration);
      // shortcut: cast assumes settings.organizations exists, as main did. revisit: when LinkedIn settings get a type
      if (
        (integration.settings as { organizations: unknown[] } | undefined)?.organizations.length ===
        1
      ) {
        notifyProgress('linkedin', integration.segmentId);
      }
      pushToIntegration({ id: integration.segmentId, grandparentId });
    } catch (error) {
      Errors.handle(error);
    }
  };

  const onboardLinkedin = async ({
    organizationId,
    segmentId: targetSegmentId,
    grandparentId,
  }: LinkedinOnboardPayload): Promise<void> => {
    try {
      const integration: IntegrationRecord = await IntegrationService.linkedinOnboard(
        organizationId,
        targetSegmentId,
      );
      upsert(integration);
      notifyProgress('linkedin', integration.segmentId);
      pushToIntegration({ id: integration.segmentId, grandparentId });
    } catch (error) {
      Errors.handle(error);
    }
  };

  const connectDiscord = async ({ guildId }: DiscordConnectPayload): Promise<void> => {
    try {
      const integration: IntegrationRecord = await IntegrationService.discordConnect(guildId);
      upsert(integration);
      notifyProgress('discord', integration.segmentId);
      pushToIntegration({ id: integration.segmentId });
    } catch (error) {
      Errors.handle(error);
    }
  };

  const connectDevto = async ({
    users,
    organizations,
    apiKey,
    segmentId: targetSegmentId,
    grandparentId,
  }: DevtoConnectPayload): Promise<void> => {
    try {
      const integration: IntegrationRecord = await IntegrationService.devtoConnect(
        users,
        organizations,
        apiKey,
        [targetSegmentId],
      );
      upsert(integration);
      notifyProgress('devto', integration.segmentId);
      pushToIntegration({ id: integration.segmentId, grandparentId });
    } catch (error) {
      Errors.handle(error);
    }
  };

  const connectHackerNews = async ({
    keywords,
    urls,
    segmentId: targetSegmentId,
    grandparentId,
  }: HackerNewsConnectPayload): Promise<void> => {
    try {
      const integration: IntegrationRecord = await IntegrationService.hackerNewsConnect(
        keywords,
        urls,
        [targetSegmentId],
      );
      upsert(integration);
      notifyProgress('hackernews', integration.segmentId);
      pushToIntegration({ id: integration.segmentId, grandparentId });
    } catch (error) {
      Errors.handle(error);
    }
  };

  const onboardStackOverflow = async ({
    tags,
    keywords,
    segmentId: targetSegmentId,
    grandparentId,
  }: StackOverflowOnboardPayload): Promise<void> => {
    try {
      const integration: IntegrationRecord = await IntegrationService.stackOverflowOnboard(
        targetSegmentId,
        tags,
        keywords,
      );
      upsert(integration);
      notifyProgress('stackoverflow', targetSegmentId);
      pushToIntegration({ id: targetSegmentId, grandparentId });
    } catch (error) {
      Errors.handle(error);
    }
  };

  return {
    integrations,
    allIds,
    count,
    loading,
    loaded,
    segmentId,
    loadingFetch,
    loadingFind,
    findById,
    findByPlatform,
    list,
    listByPlatform,
    array,
    active,
    activeList,
    inProgress,
    withErrors,
    withNoData,
    needsReconnect,
    hasRows,
    upsert,
    fetch,
    find,
    destroy,
    connectGithub,
    connectGitlab,
    connectGit,
    connectConfluence,
    connectGerrit,
    connectDiscourse,
    connectGroupsio,
    connectJira,
    onboardReddit,
    connectLinkedin,
    onboardLinkedin,
    connectDiscord,
    connectDevto,
    connectHackerNews,
    onboardStackOverflow,
  };
});
