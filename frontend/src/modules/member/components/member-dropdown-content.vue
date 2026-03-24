<template>
  <template v-if="identities.length > 1 && !props.hideUnmerge && hasPermission(LfPermission.memberEdit)">
    <button
      class="h-10 el-dropdown-menu__item w-full"
      type="button"
      @click="handleCommand({
        action: Actions.UNMERGE_IDENTITY,
        member,
      })"
    >
      <lf-icon name="link-simple-slash" class="mr-2" />
      <span class="text-xs">Unmerge identity</span>
    </button>
    <el-divider class="border-gray-200" />
  </template>

  <router-link
    v-if="!props.hideEdit && hasPermission(LfPermission.memberEdit)"
    :to="{
      name: 'memberView',
      params: {
        id: member.id,
      },
    }"
  >
    <button
      class="h-10 el-dropdown-menu__item w-full mb-1"
      type="button"
    >
      <lf-icon name="pen fa-sharp" class="mr-2" />
      <span class="text-xs">Edit profile</span>
    </button>
  </router-link>
  <button
    class="h-10 el-dropdown-menu__item w-full mb-1"
    type="button"
    :disabled="isFindingGitHubDisabled"
    @click="handleCommand({
      action: Actions.FIND_GITHUB,
      member,
    })"
  >
    <span
      class="max-w-[16px]"
      color="#9CA3AF"
    >
      <lf-icon name="github" type="brands" /></span>
    <span class="ml-2 text-xs"> Find GitHub </span>
  </button>

  <button
    v-if="!props.hideMerge && hasPermission(LfPermission.mergeMembers)"
    class="h-10 el-dropdown-menu__item w-full"
    :disabled="!hasPermission(LfPermission.mergeMembers)"
    type="button"
    @click="
      handleCommand({
        action: Actions.MERGE_CONTACT,
        member,
      })
    "
  >
    <lf-icon name="user-group" class="mr-2" />
    <span class="text-xs">Merge profile</span>
  </button>

  <template v-if="hasPermission(LfPermission.memberEdit)">
    <el-tooltip
      placement="top"
      content="Mark as team member if they belong to your own organization"
      popper-class="max-w-[260px]"
    >
      <span>
        <button
          v-if="!member.attributes?.isTeamMember?.default"
          class="h-10 el-dropdown-menu__item w-full"
          type="button"
          @click="
            handleCommand({
              action: Actions.MARK_CONTACT_AS_TEAM_CONTACT,
              member,
              value: true,
            })
          "
        >
          <lf-icon name="bookmark" class="mr-2" />
          <span class="text-xs">Mark as team member</span>
        </button>
      </span>
    </el-tooltip>
    <button
      v-if="member.attributes?.isTeamMember?.default"
      class="h-10 el-dropdown-menu__item w-full"
      type="button"
      @click="
        handleCommand({
          action: Actions.MARK_CONTACT_AS_TEAM_CONTACT,
          member,
          value: false,
        })
      "
    >
      <lf-icon name="bookmark-slash" class="mr-2" />
      <span class="text-xs">Unmark as team member</span>
    </button>
    <button
      v-if="!member.attributes.isBot?.default"
      class="h-10 el-dropdown-menu__item w-full"
      type="button"
      @click="
        handleCommand({
          action: Actions.MARK_CONTACT_AS_BOT,
          member,
        })
      "
    >
      <lf-icon name="robot" class="mr-2" />
      <span class="text-xs">Mark as bot</span>
    </button>
    <button
      v-if="member.attributes.isBot?.default"
      class="h-10 el-dropdown-menu__item w-full"
      type="button"
      @click="
        handleCommand({
          action: Actions.UNMARK_CONTACT_AS_BOT,
          member,
        })
      "
    >
      <lf-icon name="robot" class="mr-2" />
      <span class="text-xs">Unmark as bot</span>
    </button>
  </template>
  <template v-if="hasPermission(LfPermission.memberDestroy)">
    <el-divider class="border-gray-200" />
    <button
      class="h-10 el-dropdown-menu__item w-full"
      type="button"
      @click="
        handleCommand({
          action: Actions.DELETE_CONTACT,
          member,
        })
      "
    >
      <lf-icon name="trash-can" class="mr-2 text-red-500" />
      <span class="text-xs text-red-500">Delete profile</span>
    </button>
  </template>
</template>

<script setup lang="ts">
import { mapActions } from '@/shared/vuex/vuex.helpers';
import { MemberService } from '@/modules/member/member-service';

import { ToastStore } from '@/shared/message/notification';
import ConfirmDialog from '@/shared/dialog/confirm-dialog';
import { useRoute } from 'vue-router';
import { computed } from 'vue';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { useQueryClient } from '@tanstack/vue-query';
import { TanstackKey } from '@/shared/types/tanstack';
import { Member } from '../types/Member';

enum Actions {
  DELETE_CONTACT = 'deleteContact',
  SYNC_HUBSPOT = 'syncHubspot',
  STOP_SYNC_HUBSPOT = 'stopSyncHubspot',
  MARK_CONTACT_AS_TEAM_CONTACT = 'markContactAsTeamContact',
  MARK_CONTACT_AS_BOT = 'markContactAsBot',
  UNMARK_CONTACT_AS_BOT = 'unmarkContactAsBot',
  MERGE_CONTACT = 'mergeContact',
  UNMERGE_IDENTITY = 'unmergeIdentity',
  FIND_GITHUB = 'findGithub'
}

const emit = defineEmits<{(e: 'merge'): void, (e: 'unmerge'): void, (e: 'closeDropdown'): void, (e: 'findGithub'): void }>();
const props = defineProps<{
  member: Member;
  hideMerge?: boolean;
  hideEdit?: boolean;
  hideUnmerge?: boolean;
}>();

const route = useRoute();
const queryClient = useQueryClient();

const { doFind } = mapActions('member');

const { trackEvent } = useProductTracking();

const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());

const { hasPermission } = usePermissions();

const isFindingGitHubDisabled = computed(() => (
  !!props.member.username?.github
));

// Refresh member data by invalidating TanStack Query cache
// Note: Backend cache is invalidated by passing invalidateCache parameter to update/delete operations
const refreshMemberData = async () => {
  await queryClient.invalidateQueries({
    queryKey: [TanstackKey.MEMBERS_LIST],
  });
};

// Helper function to fetch member with all attributes before update
const fetchMemberWithAllAttributes = async (memberId: string) => {
  const response = await MemberService.find(
    memberId,
    selectedProjectGroup.value?.id,
    true,
  );

  return response;
};

const doManualAction = async ({
  loadingMessage,
  actionFn,
  successMessage,
  errorMessage,
}: {
  loadingMessage?: string;
  successMessage?: string;
  errorMessage?: string;
  actionFn: Promise<any>;
}) => {
  emit('closeDropdown');

  if (loadingMessage) {
    ToastStore.info(loadingMessage);
  }

  return actionFn
    .then(() => {
      if (successMessage) {
        ToastStore.closeAll();
        ToastStore.success(successMessage);
      }
      Promise.resolve();
    })
    .catch(() => {
      if (errorMessage) {
        ToastStore.closeAll();
        ToastStore.error(errorMessage);
      }
      Promise.reject();
    });
};

const handleCommand = async (command: {
  action: Actions;
  member: Member;
  value?: boolean;
}) => {
  // Delete contact
  if (command.action === Actions.DELETE_CONTACT) {
    ConfirmDialog({
      type: 'danger',
      title: 'Delete profile',
      message: "Are you sure you want to proceed? You can't undo this action",
      confirmButtonText: 'Confirm',
      cancelButtonText: 'Cancel',
      icon: 'fa-trash-can fa-light',
    }).then(() => {
      trackEvent({
        key: FeatureEventKey.DELETE_MEMBER,
        type: EventType.FEATURE,
        properties: {
          path: route.path,
        },
      });

      doManualAction({
        loadingMessage: 'Profile is being deleted',
        successMessage: 'Profile successfully deleted',
        errorMessage: 'Something went wrong',
        actionFn: MemberService.destroyAll([command.member.id]),
      }).then(async () => {
        await refreshMemberData();
      });
    });

    return;
  }

  // Sync with hubspot
  if (
    command.action === Actions.SYNC_HUBSPOT
    || command.action === Actions.STOP_SYNC_HUBSPOT
  ) {
    const isSyncing = command.action === Actions.SYNC_HUBSPOT;

    doManualAction({
      loadingMessage: isSyncing
        ? 'Profile is being synced with Hubspot'
        : 'Profile syncing with Hubspot is being stopped',
      successMessage: isSyncing
        ? 'Profile is now syncing with HubSpot'
        : 'Profile syncing stopped',
      errorMessage: 'Something went wrong',
      actionFn: isSyncing
        ? HubspotApiService.syncMember(command.member.id)
        : HubspotApiService.stopSyncMember(command.member.id),
    }).then(async () => {
      if (route.name === 'member') {
        await refreshMemberData();
      } else {
        doFind({
          id: command.member.id,
          segments: [selectedProjectGroup.value?.id],
        });
      }
    });

    return;
  }

  // Mark as team contact
  if (command.action === Actions.MARK_CONTACT_AS_TEAM_CONTACT) {
    trackEvent({
      key: FeatureEventKey.MARK_AS_TEAM_MEMBER,
      type: EventType.FEATURE,
      properties: {
        path: route.path,
      },
    });

    // Fetch member with all attributes to prevent data loss
    const memberWithAllAttributes = await fetchMemberWithAllAttributes(command.member.id);
    const currentAttributes = memberWithAllAttributes.attributes;

    doManualAction({
      loadingMessage: 'Profile is being updated',
      successMessage: 'Profile updated successfully',
      errorMessage: 'Something went wrong',
      actionFn: MemberService.update(command.member.id, {
        attributes: {
          ...currentAttributes,
          isTeamMember: {
            default: command.value,
            custom: command.value,
          },
        },
        invalidateCache: true,
      }),
    }).then(async () => {
      await refreshMemberData();
    });

    return;
  }

  // Mark as bot
  if (
    command.action === Actions.MARK_CONTACT_AS_BOT
    || command.action === Actions.UNMARK_CONTACT_AS_BOT
  ) {
    trackEvent({
      key: FeatureEventKey.MARK_AS_BOT,
      type: EventType.FEATURE,
      properties: {
        path: route.path,
      },
    });

    // Fetch member with all attributes to prevent data loss
    const memberWithAllAttributes = await fetchMemberWithAllAttributes(command.member.id);
    const currentAttributes = memberWithAllAttributes.attributes;

    const isBot = command.action === Actions.MARK_CONTACT_AS_BOT;

    doManualAction({
      loadingMessage: 'Profile is being updated',
      successMessage: 'Profile updated successfully',
      errorMessage: 'Something went wrong',
      actionFn: MemberService.update(command.member.id, {
        attributes: {
          ...currentAttributes,
          isBot: {
            ...currentAttributes.isBot,
            default: isBot,
            custom: isBot,
          },
        },
        invalidateCache: true,
      }),
    }).then(async () => {
      await refreshMemberData();
    });

    return;
  }

  // Merge contact
  if (command.action === Actions.MERGE_CONTACT) {
    emit('closeDropdown');
    emit('merge');

    return;
  }

  // Merge contact
  if (command.action === Actions.UNMERGE_IDENTITY) {
    emit('closeDropdown');
    emit('unmerge');

    return;
  }

  if (command.action === Actions.FIND_GITHUB) {
    trackEvent({
      key: FeatureEventKey.FIND_IDENTITY,
      type: EventType.FEATURE,
    });

    emit('closeDropdown');
    emit('findGithub');

    return;
  }

  emit('closeDropdown');
};

const identities = computed(() => (props.member.identities || []).filter((i) => i.type !== 'email'));
</script>

<style lang="scss" scoped>
.el-dropdown__popper .el-dropdown__list {
  @apply p-2;
}

// Override divider margin
.el-divider--horizontal {
  @apply my-2;
}

.el-dropdown-menu__item:disabled {
  @apply cursor-not-allowed text-gray-400;
}

.el-dropdown-menu__item:disabled:hover {
  @apply bg-white;
}
</style>
