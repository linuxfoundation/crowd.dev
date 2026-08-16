import { IntegrationService } from '@/modules/integration/integration-service';
import Errors from '@/shared/error/errors';
import { router } from '@/router';
import { isCurrentDateAfterGivenWorkingDays } from '@/utils/date';
import { showIntegrationProgressNotification } from '@/modules/integration/helpers/integration-progress-notification';
import { lfIntegrations } from '@/config/integrations';
import { ToastStore } from '@/shared/message/notification';

export const ERROR_BANNER_WORKING_DAYS_DISPLAY = 3;

const getNangoStatus = (integration) => {
  const reposObj = integration.settings.nangoMapping;
  const mapped = reposObj ? Object.values(reposObj) : [];
  const allRepos = integration.settings.orgs.map((org) => org.repos).flat();
  return mapped.length < allRepos.length ? 'in-progress' : 'done';
};

export default {
  namespaced: true,

  state: () => ({
    segmentId: null,
    byId: {},
    allIds: [],
    count: 0,
    loading: false,
    loaded: false,
  }),

  getters: {
    loadingFetch: (state) => state.loading,

    loadingFind: (state) => (id) => state.byId[id]?.loading,

    loaded: (state) => state.loaded,

    find: (state) => (id) => state.byId[id],

    findByPlatform: (state, getters) => (platform) => getters.array.find((w) => w.platform === platform),

    list: (state) => Object.keys(state.byId).reduce((acc, key) => {
      acc[key] = {
        ...state.byId[key],
        ...lfIntegrations()[state.byId[key].platform],
      };
      return acc;
    }, {}),

    listByPlatform: (state) => Object.keys(state.byId).reduce((acc, key) => {
      acc[state.byId[key].platform] = {
        ...state.byId[key],
        ...lfIntegrations()[state.byId[key].platform],
      };
      return acc;
    }, {}),

    array: (state, getters) => state.allIds.map((id) => getters.list[id]),

    active: (state, getters) => getters.array.filter(
      (i) => i.status === 'done' || i.status === 'in-progress',
    ),

    activeList: (state, getters) => getters.active.reduce((acc, item) => {
      acc[item.platform] = item;
      return acc;
    }, {}),

    inProgress: (state, getters) => getters.array.filter((i) => i.status === 'in-progress'),

    withErrors: (state, getters) => getters.array.filter(
      (i) => i.status === 'error'
          && isCurrentDateAfterGivenWorkingDays(
            i.updatedAt,
            ERROR_BANNER_WORKING_DAYS_DISPLAY,
          ),
    ),

    withNoData: (state, getters) => getters.array.filter((i) => i.status === 'no-data'),

    needsReconnect: (state, getters) => getters.array.filter((i) => i.status === 'needs-reconnect'),

    count: (state) => state.count,

    hasRows: (state, getters) => getters.count > 0,
  },

  mutations: {
    FETCH_STARTED(state) {
      state.loading = true;
    },

    FETCH_SUCCESS(state, payload) {
      state.loading = false;

      const byId = {};
      const allIds = [];

      payload.rows.forEach((integration) => {
        byId[integration.id] = integration;
        if (allIds.indexOf(integration.id) === -1) {
          allIds.push(integration.id);
        }
      });

      state.segmentId = router.currentRoute.value.params.id;
      state.byId = byId;
      state.allIds = allIds;
      state.count = payload.count;
      state.loaded = true;
    },

    FETCH_ERROR(state) {
      state.loading = false;
      state.rows = [];
      state.count = 0;
    },

    FIND_STARTED(state, id) {
      if (state.byId[id]) {
        state.byId[id].loading = true;
      }
    },

    FIND_SUCCESS(state, record) {
      if (!record) {
        return;
      }
      state.byId[record.id] = record;
      state.byId[record.id].loading = false;
      if (state.allIds.indexOf(record.id) === -1) {
        state.allIds.push(record.id);
      }
    },

    FIND_ERROR(state, id) {
      if (state.byId[id]) {
        state.byId[id].loading = false;
      }
    },

    CREATE_STARTED() {},

    CREATE_SUCCESS(state, record) {
      state.byId[record.id] = record;
      if (state.allIds.indexOf(record.id) === -1) {
        state.allIds.push(record.id);
        state.count += 1;
      }
    },

    CREATE_ERROR() {},

    UPDATE_STARTED() {},

    UPDATE_SUCCESS(state, record) {
      state.byId[record.id] = record;
      if (state.allIds.indexOf(record.id) === -1) {
        state.allIds.push(record.id);
        state.count += 1;
      }
    },

    UPDATE_ERROR() {},

    DESTROY_STARTED(state) {
      state.loading = true;
    },

    DESTROY_SUCCESS(state, id) {
      state.loading = false;
      delete state.byId[id];
      const index = state.allIds.indexOf(id);
      state.allIds.splice(index, 1);
      state.count -= 1;
    },

    DESTROY_ERROR(state) {
      state.loading = false;
    },

    DESTROY_ALL_STARTED(state) {
      state.loading = true;
    },

    DESTROY_ALL_SUCCESS(state) {
      state.loading = false;
      state.byId = {};
      state.allIds.splice(0);
      state.count = 0;
    },

    DESTROY_ALL_ERROR(state) {
      state.loading = false;
    },
  },

  actions: {
    async doFetch({ commit }, segments = []) {
      try {
        commit('FETCH_STARTED');

        const response = await IntegrationService.list(
          null,
          null,
          null,
          null,
          segments,
        );

        // This is a temporary workaround while we have to support 2 github integrations
        const parsedRows = response.rows.map((row) => {
          if (row.platform === 'github-nango') {
            return {
              ...row,
              platform: 'github',
              isNango: true,
              status: getNangoStatus(row),
            };
          }

          return row;
        });

        commit('FETCH_SUCCESS', {
          rows: parsedRows,
          count: response.count,
        });
      } catch (error) {
        Errors.handle(error);
        commit('FETCH_ERROR');
      }
    },

    async doDestroy({ commit, dispatch }, integrationId) {
      try {
        commit('DESTROY_STARTED');

        await IntegrationService.destroyAll([integrationId]);
        ToastStore.success('Integration was disconnected successfully');

        commit('DESTROY_SUCCESS', integrationId);
        dispatch('doFetch');
      } catch (error) {
        Errors.handle(error);
        commit('DESTROY_ERROR');
      }
    },

    async doDestroyAll({ commit }, integrationIds) {
      try {
        commit('DESTROY_ALL_STARTED');

        const response = await IntegrationService.destroyAll(integrationIds);

        commit('DESTROY_ALL_SUCCESS', response);
      } catch (error) {
        Errors.handle(error);
        commit('DESTROY_ALL_ERROR');
      }
    },

    async doFind({ commit }, id) {
      try {
        commit('FIND_STARTED', id);
        const record = await IntegrationService.find(id);
        commit('FIND_SUCCESS', record);
      } catch (error) {
        Errors.handle(error);
        commit('FIND_ERROR', id);
      }
    },

    async doGithubConnect(
      { commit, dispatch },
      { code, installId, setupAction },
    ) {
      // Function to trigger Oauth performance.
      try {
        commit('CREATE_STARTED');
        // Call the connect function in IntegrationService to handle functionality
        const integration = await IntegrationService.githubConnect(
          code,
          installId,
          setupAction,
        );
        const repos = integration?.settings?.repos || [];
        const data = repos.reduce(
          (a, b) => ({
            ...a,
            [b.url]: integration.segmentId,
          }),
          {},
        );

        dispatch('doFetch');

        router.push({
          name: 'integration',
          params: {
            id: integration.segmentId,
          },
        });

        dispatch('doFetch', [integration.segmentId]);
      } catch (error) {
        Errors.handle(error);
        commit('CREATE_ERROR');
      }
    },

    async doRedditOnboard(
      { commit },
      { subreddits, segmentId, grandparentId },
    ) {
      // Function to trigger Oauth performance.
      try {
        commit('CREATE_STARTED');
        // Call the connect function in IntegrationService to handle functionality
        const integration = await IntegrationService.redditOnboard(
          subreddits,
          segmentId,
        );

        commit('CREATE_SUCCESS', integration);

        showIntegrationProgressNotification('reddit', integration.segmentId);
        router.push({
          name: 'integration',
          params: {
            id: segmentId,
            grandparentId,
          },
        });
      } catch (error) {
        Errors.handle(error);
        commit('CREATE_ERROR');
      }
    },

    async doLinkedinConnect({ commit }, { segmentId, grandparentId }) {
      try {
        commit('CREATE_STARTED');
        // Call the connect function in IntegrationService to handle functionality
        const integration = await IntegrationService.linkedinConnect(segmentId);

        commit('CREATE_SUCCESS', integration);
        if (integration.settings?.organizations.length === 1) {
          showIntegrationProgressNotification(
            'linkedin',
            integration.segmentId,
          );
        }
        router.push({
          name: 'integration',
          params: {
            id: integration.segmentId,
            grandparentId,
          },
        });
      } catch (error) {
        Errors.handle(error);
        commit('CREATE_ERROR');
      }
    },

    async doLinkedinOnboard(
      { commit },
      { organizationId, segmentId, grandparentId },
    ) {
      try {
        commit('UPDATE_STARTED');
        // Call the connect function in IntegrationService to handle functionality
        const integration = await IntegrationService.linkedinOnboard(
          organizationId,
          segmentId,
        );

        commit('UPDATE_SUCCESS', integration);

        showIntegrationProgressNotification('linkedin', integration.segmentId);

        router.push({
          name: 'integration',
          params: {
            id: integration.segmentId,
            grandparentId,
          },
        });
      } catch (error) {
        Errors.handle(error);
        commit('UPDATE_ERROR');
      }
    },

    async doDiscordConnect({ commit }, { guildId }) {
      // Function to connect to Discord. We just need to store the
      // guildId to be able to match bot events to users.
      try {
        commit('CREATE_STARTED');

        const integration = await IntegrationService.discordConnect(guildId);

        commit('CREATE_SUCCESS', integration);

        showIntegrationProgressNotification('discord', integration.segmentId);
        router.push({
          name: 'integration',
          params: {
            id: integration.segmentId,
          },
        });
      } catch (error) {
        Errors.handle(error);
        commit('CREATE_ERROR');
      }
    },

    async doDevtoConnect(
      { commit },
      {
        users, organizations, apiKey, segmentId, grandparentId,
      },
    ) {
      // Function to connect to Dev.to. We just need to store the
      // users and organizations we want to track

      try {
        commit('CREATE_STARTED');

        const integration = await IntegrationService.devtoConnect(
          users,
          organizations,
          apiKey,
          [segmentId],
        );

        commit('CREATE_SUCCESS', integration);

        showIntegrationProgressNotification('devto', integration.segmentId);

        router.push({
          name: 'integration',
          params: {
            id: integration.segmentId,
            grandparentId,
          },
        });
      } catch (error) {
        Errors.handle(error);
        commit('CREATE_ERROR');
      }
    },

    async doHackerNewsConnect(
      { commit },
      {
        keywords, urls, segmentId, grandparentId,
      },
    ) {
      // Function to connect to Dev.to. We just need to store the
      // users and organizations we want to track

      try {
        commit('CREATE_STARTED');

        const integration = await IntegrationService.hackerNewsConnect(
          keywords,
          urls,
          [segmentId],
        );

        commit('CREATE_SUCCESS', integration);

        showIntegrationProgressNotification(
          'hackernews',
          integration.segmentId,
        );

        router.push({
          name: 'integration',
          params: {
            id: integration.segmentId,
            grandparentId,
          },
        });
      } catch (error) {
        Errors.handle(error);
        commit('CREATE_ERROR');
      }
    },

    async doStackOverflowOnboard(
      { commit },
      {
        tags, keywords, segmentId, grandparentId,
      },
    ) {
      // Function to connect to StackOverflow.

      try {
        commit('CREATE_STARTED');

        const integration = await IntegrationService.stackOverflowOnboard(
          segmentId,
          tags,
          keywords,
        );

        commit('CREATE_SUCCESS', integration);

        showIntegrationProgressNotification('stackoverflow', segmentId);

        router.push({
          name: 'integration',
          params: {
            id: segmentId,
            grandparentId,
          },
        });
      } catch (error) {
        Errors.handle(error);
        commit('CREATE_ERROR');
      }
    },

    async doHubspotConnect({ commit }) {
      try {
        commit('CREATE_STARTED');

        const integration = await IntegrationService.hubspotConnect();

        commit('CREATE_SUCCESS', integration);

        router.push('/integrations');
      } catch (error) {
        Errors.handle(error);
        commit('CREATE_ERROR');
      }
    },

    async doGitConnect(
      { commit },
      {
        remotes, isUpdate, segmentId, grandparentId, errorHandler,
      },
    ) {
      try {
        commit('CREATE_STARTED');

        const integration = await IntegrationService.gitConnect(remotes, [
          segmentId,
        ]);

        commit('CREATE_SUCCESS', integration);

        ToastStore.success(
          'The first activities will show up in a couple of seconds. <br /> <br /> '
            + 'This process might take a few minutes to finish, depending on the amount of data.',
          {
            title: `Git integration ${
              isUpdate ? 'updated' : 'created'
            } successfully`,
          },
        );

        router.push({
          name: 'integration',
          params: {
            id: segmentId,
            grandparentId,
          },
        });
      } catch (error) {
        if (errorHandler) {
          errorHandler(error);
        } else {
          Errors.handle(error);
        }
        commit('CREATE_ERROR');
      }
    },

    async doConfluenceConnect(
      { commit },
      {
        id, settings, isUpdate, segmentId, grandparentId,
      },
    ) {
      try {
        commit('CREATE_STARTED');

        const integration = await IntegrationService.confluenceConnect(
          id,
          settings,
          segmentId,
        );

        commit('CREATE_SUCCESS', integration);

        ToastStore.success(
          'The first activities will show up in a couple of seconds. <br /> <br /> '
            + 'This process might take a few minutes to finish, depending on the amount of data.',
          {
            title: `Confluence integration ${
              isUpdate ? 'updated' : 'created'
            } successfully`,
          },
        );

        router.push({
          name: 'integration',
          params: {
            id: segmentId,
            grandparentId,
          },
        });
      } catch (error) {
        Errors.handle(error);
        commit('CREATE_ERROR');
      }
    },

    async doSchedConnect(
      { commit },
      {
        id, settings, isUpdate, segmentId, grandparentId,
      },
    ) {
      try {
        commit('CREATE_STARTED');

        const integration = await IntegrationService.schedConnect(
          id,
          settings,
          segmentId,
        );

        commit('CREATE_SUCCESS', integration);

        ToastStore.success(
          'The first activities will show up in a couple of seconds. <br /> <br /> '
            + 'This process might take a few minutes to finish, depending on the amount of data.',
          {
            title: `Sched integration ${
              isUpdate ? 'updated' : 'created'
            } successfully`,
          },
        );

        router.push({
          name: 'integration',
          params: {
            id: segmentId,
            grandparentId,
          },
        });
      } catch (error) {
        Errors.handle(error);
        commit('CREATE_ERROR');
      }
    },

    async doGerritConnect(
      { commit, dispatch },
      {
        orgURL,
        // user,
        // pass,
        isUpdate,
        repoNames,
        enableAllRepos,
        enableGit,
        segmentId,
        grandparentId,
        errorHandler,
      },
    ) {
      try {
        commit('CREATE_STARTED');

        const integration = await IntegrationService.gerritConnect(
          {
            orgURL,
            // user,
            // pass,
            repoNames,
            enableAllRepos,
            enableGit,
          },
          [segmentId],
        );

        commit('CREATE_SUCCESS', integration);

        ToastStore.success(
          'The first activities will show up in a couple of seconds. <br /> <br /> '
            + 'This process might take a few minutes to finish, depending on the amount of data.',
          {
            title: `Gerrit integration ${
              isUpdate ? 'updated' : 'created'
            } successfully`,
          },
        );

        // Refresh integrations to update Git integration on UI
        dispatch('doFetch', [segmentId]);

        router.push({
          name: 'integration',
          params: {
            id: integration.segmentId,
            grandparentId,
          },
        });
      } catch (error) {
        if (errorHandler) {
          errorHandler(error);
        } else {
          Errors.handle(error);
        }
        commit('CREATE_ERROR');
      }
    },

    async doDiscourseConnect(
      { commit },
      {
        forumHostname,
        apiKey,
        webhookSecret,
        isUpdate,
        segmentId,
        grandparentId,
      },
    ) {
      try {
        commit('CREATE_STARTED');

        const integration = await IntegrationService.discourseConnect(
          forumHostname,
          apiKey,
          webhookSecret,
          [segmentId],
        );

        commit('CREATE_SUCCESS', integration);

        ToastStore.success(
          'The first activities will show up in a couple of seconds. <br /> <br /> '
            + 'This process might take a few minutes to finish, depending on the amount of data.',
          {
            title: `Discourse integration ${
              isUpdate ? 'updated' : 'created'
            } successfully`,
          },
        );

        router.push({
          name: 'integration',
          params: {
            id: integration.segmentId,
            grandparentId,
          },
        });
      } catch (error) {
        Errors.handle(error);
        commit('CREATE_ERROR');
      }
    },

    async doGroupsioConnect(
      { commit },
      {
        email,
        token,
        tokenExpiry,
        password,
        groups,
        isUpdate,
        autoImports,
        segmentId,
        grandparentId,
      },
    ) {
      try {
        commit('CREATE_STARTED');

        const integration = await IntegrationService.groupsioConnect(
          email,
          token,
          tokenExpiry,
          password,
          groups,
          autoImports,
          [segmentId],
        );

        commit('CREATE_SUCCESS', integration);

        ToastStore.success(
          'The first activities will show up in a couple of seconds. <br /> <br /> '
            + 'This process might take a few minutes to finish, depending on the amount of data.',
          {
            title: `
              groups.io integration ${
  isUpdate ? 'updated' : 'created'
} successfully`,
          },
        );

        router.push({
          name: 'integration',
          params: {
            id: integration.segmentId,
            grandparentId,
          },
        });
      } catch (error) {
        Errors.handle(error);
        ToastStore.error('Something went wrong. Please try again later.');
        commit('CREATE_ERROR');
      }
    },

    async doJiraConnect(
      { commit },
      {
        id,
        url,
        username,
        personalAccessToken,
        apiToken,
        projects,
        isUpdate,
        segmentId,
        grandparentId,
      },
    ) {
      try {
        commit('CREATE_STARTED');

        const integration = await IntegrationService.jiraConnect(
          id,
          url,
          username,
          personalAccessToken,
          apiToken,
          projects,
          [segmentId],
        );

        commit('CREATE_SUCCESS', integration);

        ToastStore.success(
          'The first activities will show up in a couple of seconds. <br /> <br /> '
            + 'This process might take a few minutes to finish, depending on the amount of data.',
          {
            title: `
              Jira integration ${
  isUpdate ? 'updated' : 'created'
} successfully`,
          },
        );

        router.push({
          name: 'integration',
          params: {
            id: integration.segmentId,
            grandparentId,
          },
        });
      } catch (error) {
        Errors.handle(error);
        commit('CREATE_ERROR');
      }
    },

    async doGitlabConnect(
      { commit, dispatch },
      {
        code, state, segmentId, grandparentId,
      },
    ) {
      try {
        commit('CREATE_STARTED');
        const integration = await IntegrationService.gitlabConnect(
          code,
          state,
          [segmentId],
        );
        commit('CREATE_SUCCESS', integration);
        dispatch('doFetch');

        router.push({
          name: 'integration',
          params: {
            id: integration.segmentId,
            grandparentId,
          },
        });

        dispatch('doFetch', [integration.segmentId]);
      } catch (error) {
        Errors.handle(error);
        commit('CREATE_ERROR');
      }
    },
    async mapGitlabRepos(integrationId, mapping) {
      try {
        await IntegrationService.mapGitlabRepos(integrationId, mapping);
        await this.find(integrationId);
        ToastStore.success('GitLab repositories mapped successfully');
      } catch (error) {
        console.error('Error mapping GitLab repositories:', error);
        ToastStore.error('Failed to map GitLab repositories');
      }
    },
  },
};
