import { IntegrationService } from '@/modules/integration/integration-service';
import Errors from '@/shared/error/errors';
import { router } from '@/router';
import { showIntegrationProgressNotification } from '@/modules/integration/helpers/integration-progress-notification';
import { ToastStore } from '@/shared/message/notification';
import { useIntegrationStore } from '@/modules/integration/store/integration.store';

export default {
  namespaced: true,

  state: () => ({}),

  mutations: {
    CREATE_STARTED() {},

    CREATE_SUCCESS(state, record) {
      useIntegrationStore().upsert(record);
    },

    CREATE_ERROR() {},

    UPDATE_STARTED() {},

    UPDATE_SUCCESS(state, record) {
      useIntegrationStore().upsert(record);
    },

    UPDATE_ERROR() {},
  },

  actions: {
    doFetch(context, segments = []) {
      return useIntegrationStore().fetch(segments);
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
