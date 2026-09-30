import io from 'socket.io-client';
import { storeToRefs } from 'pinia';
import { h } from 'vue';
import config from '@/config';

import { ToastStore } from '@/shared/message/notification';
import type { User } from '@/modules/auth/types/User.type';

let socketIoClient: any;

const SocketEvents = {
  connect: 'connect',
  disconnect: 'disconnect',
  integrationCompleted: 'integration-completed',
  orgMerge: 'org-merge',
  memberMerge: 'member-merge',
  memberUnmerge: 'member-unmerge',
  organizationUnmerge: 'organization-unmerge',
};

export const isSocketConnected = () => socketIoClient && socketIoClient.connected;

export const connectSocket = (token: string, getUser: () => User | null) => {
  if (socketIoClient && socketIoClient.connected) {
    socketIoClient.disconnect();
  }

  const getSocketPath = () => {
    const { env } = config;
    const path = env === 'production' || env === 'staging' ? '/api/socket.io' : '/socket.io';
    console.info(`Socket.IO connecting to: ${path}`);

    return path;
  };

  socketIoClient = io(`${config.websocketsUrl}/user`, {
    path: getSocketPath(),
    query: {
      token,
    },
    transports: ['websocket'],
    forceNew: true,
  });

  socketIoClient.on(SocketEvents.connect, () => {
    console.info('Socket connected');
  });

  socketIoClient.on(SocketEvents.disconnect, () => {
    console.info('Socket disconnected');
  });

  socketIoClient.on(SocketEvents.integrationCompleted, (data) => {
    console.info('Integration onboarding done', data);
    // store.dispatch(
    //   'integration/doFind',
    //   JSON.parse(data).integrationId,
    // );
  });

  socketIoClient.on(SocketEvents.memberMerge, async (data) => {
    const parsedData = JSON.parse(data);
    if (!parsedData.success) {
      return;
    }
    try {
      const { useLfSegmentsStore } = await import('@/modules/lf/segments/store');
      const lsSegmentsStore = useLfSegmentsStore();
      const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);
      const {
        primaryDisplayName,
        secondaryDisplayName,
        primaryId,
        secondaryId,
        userId,
      } = parsedData;

      if (getUser()?.id !== userId) {
        return;
      }

      const primaryMember = h(
        'a',
        {
          href: `${window.location.origin}/people/${primaryId}?projectGroup=${selectedProjectGroup.value?.id}`,
          class: 'underline text-gray-600',
        },
        primaryDisplayName,
      );
      const secondaryMember = h(
        'a',
        {
          href: `${window.location.origin}/people/${secondaryId}?projectGroup=${selectedProjectGroup.value?.id}`,
          class: 'underline text-gray-600',
        },
        secondaryDisplayName,
      );
      const between = h(
        'span',
        {},
        ' merged into ',
      );
      const after = h(
        'span',
        {},
        '. Finalizing profile merging might take some time to complete.',
      );
      ToastStore.closeAll();
      ToastStore.success(h(
        'div',
        {},
        [secondaryMember, between, primaryMember, after],
      ), {
        title: 'Profiles merged successfully',
      });
    } catch (error) {
      console.error('Socket memberMerge: failed to load segments module', error);
    }
  });

  socketIoClient.on(SocketEvents.memberUnmerge, async (data) => {
    console.info('Member unmerge done', data);
    const parsedData = JSON.parse(data);
    if (!parsedData.success) {
      return;
    }
    try {
      const { useLfSegmentsStore } = await import('@/modules/lf/segments/store');
      const lsSegmentsStore = useLfSegmentsStore();
      const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);
      const {
        primaryDisplayName,
        secondaryDisplayName,
        primaryId,
        secondaryId,
        userId,
      } = parsedData;

      if (getUser()?.id !== userId) {
        return;
      }

      const primaryMember = h(
        'a',
        {
          href: `${window.location.origin}/people/${primaryId}?projectGroup=${selectedProjectGroup.value?.id}`,
          class: 'underline text-gray-600',
        },
        primaryDisplayName,
      );
      const secondaryMember = h(
        'a',
        {
          href: `${window.location.origin}/people/${secondaryId}?projectGroup=${selectedProjectGroup.value?.id}`,
          class: 'underline text-gray-600',
        },
        secondaryDisplayName,
      );
      const between = h(
        'span',
        {},
        ' unmerged from ',
      );
      const after = h(
        'span',
        {},
        '. Finalizing profile unmerging might take some time to complete.',
      );
      ToastStore.closeAll();
      ToastStore.success(h(
        'div',
        {},
        [secondaryMember, between, primaryMember, after],
      ), {
        title: 'Profiles unmerged successfully',
      });
    } catch (error) {
      console.error('Socket memberUnmerge: failed to load segments module', error);
    }
  });

  socketIoClient.on(SocketEvents.organizationUnmerge, async (data) => {
    console.info('Organization unmerge done', data);
    const parsedData = JSON.parse(data);
    if (!parsedData.success) {
      return;
    }
    try {
      const { useLfSegmentsStore } = await import('@/modules/lf/segments/store');
      const lsSegmentsStore = useLfSegmentsStore();
      const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);
      const {
        primaryDisplayName, secondaryDisplayName, primaryId, secondaryId, userId,
      } = parsedData;

      if (getUser()?.id !== userId) {
        return;
      }

      const primaryOrganization = h(
        'a',
        {
          href: `${window.location.origin}/organizations/${primaryId}?projectGroup=${selectedProjectGroup.value?.id}`,
          class: 'underline text-gray-600',
        },
        primaryDisplayName,
      );
      const secondaryOrganization = h(
        'a',
        {
          href: `${window.location.origin}/organizations/${secondaryId}?projectGroup=${selectedProjectGroup.value?.id}`,
          class: 'underline text-gray-600',
        },
        secondaryDisplayName,
      );
      const between = h(
        'span',
        {},
        ' unmerged from ',
      );
      const after = h(
        'span',
        {},
        '. Syncing organization activities might take some time to complete.',
      );
      ToastStore.closeAll();
      ToastStore.success(h(
        'div',
        {},
        [secondaryOrganization, between, primaryOrganization, after],
      ), {
        title: 'Organizations unmerged successfully',
      });
    } catch (error) {
      console.error('Socket organizationUnmerge: failed to load segments module', error);
    }
  });

  socketIoClient.on(SocketEvents.orgMerge, async (payload) => {
    const {
      success,
      userId,
      primaryOrgId,
      original,
      toMerge,
    } = JSON.parse(payload);

    if (getUser().id !== userId) {
      return;
    }

    try {
      const { useOrganizationStore } = await import('@/modules/organization/store/pinia');
      const { removeMergedOrganizations } = useOrganizationStore();
      const primaryOrganization = {
        id: primaryOrgId,
        displayName: original,
      };
      const secondaryOrganization = {
        displayName: toMerge,
      };

      ToastStore.closeAll();

      removeMergedOrganizations(primaryOrgId);

      const { default: useOrganizationMergeMessage } = await import('@/shared/modules/merge/config/useOrganizationMergeMessage');
      const { successMessage, socketErrorMessage } = useOrganizationMergeMessage;
      if (success) {
        successMessage({
          primaryOrganization,
          secondaryOrganization,
        });
      } else {
        socketErrorMessage({
          primaryOrganization,
          secondaryOrganization,
        });
      }
    } catch (error) {
      console.error('Socket orgMerge: failed to load organization module', error);
    }
  });
};

export const disconnectSocket = () => {
  if (socketIoClient) {
    socketIoClient.disconnect();
  }
};
