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
  },
};
