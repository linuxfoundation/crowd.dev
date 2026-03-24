<template>
  <lf-table-bulk-actions :selected-items="selectedMembers">
    <el-dropdown trigger="click" @command="handleCommand">
      <lf-button
        type="secondary-gray"
        size="small"
      >
        <span class="mr-2">Actions</span>
        <lf-icon name="chevron-down" :size="24" />
      </lf-button>
      <template #dropdown>
        <el-tooltip
          v-if="selectedMembers.length === 2 && hasPermission(LfPermission.mergeMembers)"
          content="Coming soon"
          placement="top"
        >
          <span>
            <el-dropdown-item
              :command="{ action: 'mergeMembers' }"
              :disabled="!hasPermission(LfPermission.mergeMembers)"
            >
              <lf-icon name="user-group" :size="20" class="mr-1" />
              Merge profile
            </el-dropdown-item>
          </span>
        </el-tooltip>
        <el-dropdown-item
          v-if="hasPermission(LfPermission.memberEdit)"
          :command="{
            action: 'markAsTeamMember',
            value: markAsTeamMemberOptions.value,
          }"
        >
          <lf-icon :name="markAsTeamMemberOptions.icon" :size="20" class="mr-1" />
          {{ markAsTeamMemberOptions.copy }}
        </el-dropdown-item>
        <el-dropdown-item
          v-if="hasPermission(LfPermission.memberEdit)"
          :command="{
            action: 'markAsBot',
            value: markAsBotOptions.value,
          }"
        >
          <lf-icon :name="markAsBotOptions.icon" :size="20" class="mr-1" />
          {{ markAsBotOptions.copy }}
        </el-dropdown-item>
        <el-dropdown-item
          v-if="hasPermission(LfPermission.memberEdit)"
          :command="{ action: 'editAttribute' }"
        >
          <lf-icon name="file-pen" :size="20" class="mr-1" />
          Edit attribute
        </el-dropdown-item>
        <template v-if="hasPermission(LfPermission.memberDestroy)">
          <hr class="border-gray-200 my-1 mx-2" />
          <el-dropdown-item
            :command="{ action: 'destroyAll' }"
          >
            <div
              class="flex items-center text-red-500"
            >
              <lf-icon name="trash-can" :size="20" class="mr-2" />
              Delete
            </div>
          </el-dropdown-item>
        </template>
      </template>
    </el-dropdown>
  </lf-table-bulk-actions>

  <app-bulk-edit-attribute-popover
    v-model="bulkAttributesUpdateVisible"
  />
</template>

<script setup>
import { computed, ref } from 'vue';
import { useRoute } from 'vue-router';
import { storeToRefs } from 'pinia';
import pluralize from 'pluralize';
import { useMemberStore } from '@/modules/member/store/pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { MemberService } from '@/modules/member/member-service';
import ConfirmDialog from '@/shared/dialog/confirm-dialog';

import { ToastStore } from '@/shared/message/notification';
import { showExportDialog } from '@/modules/member/member-export-limit';
import AppBulkEditAttributePopover from '@/modules/member/components/bulk/bulk-edit-attribute-popover.vue';
import useMemberMergeMessage from '@/shared/modules/merge/config/useMemberMergeMessage';
import { useAuthStore } from '@/modules/auth/store/auth.store';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfTableBulkActions from '@/ui-kit/table/table-bulk-actions.vue';
import { useQueryClient } from '@tanstack/vue-query';
import { TanstackKey } from '@/shared/types/tanstack';

const { trackEvent } = useProductTracking();

const route = useRoute();
const queryClient = useQueryClient();

const authStore = useAuthStore();
const { getUser } = authStore;

const memberStore = useMemberStore();
const { selectedMembers, filters } = storeToRefs(memberStore);

const { hasPermission } = usePermissions();

const bulkAttributesUpdateVisible = ref(false);

// Refresh member data by invalidating TanStack Query cache
// Note: Backend cache is invalidated by passing invalidateCache parameter to update/delete operations
const refreshMemberData = async () => {
  await queryClient.invalidateQueries({
    queryKey: [TanstackKey.MEMBERS_LIST],
  });
};

// Helper function to fetch member with all attributes before bulk update
const fetchMemberWithAllAttributes = async (memberId) => {
  const lsSegmentsStore = useLfSegmentsStore();
  const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

  const response = await MemberService.find(
    memberId,
    selectedProjectGroup.value?.id,
    true,
  );

  return response;
};

const markAsTeamMemberOptions = computed(() => {
  const isTeamView = filters.value.settings.teamMember === 'filter';
  const membersCopy = pluralize(
    'person',
    selectedMembers.value.length,
    false,
  );

  if (isTeamView) {
    return {
      icon: 'bookmark-slash',
      copy: `Unmark as team ${membersCopy}`,
      value: false,
    };
  }

  return {
    icon: 'bookmark',
    copy: `Mark as team ${membersCopy}`,
    value: true,
  };
});

const markAsBotOptions = computed(() => {
  const membersCopy = pluralize(
    'person',
    selectedMembers.value.length,
    false,
  );

  // Check if any of the selected members is already marked as bot
  const hasBot = selectedMembers.value.some((member) => member.attributes?.isBot?.default);

  if (hasBot) {
    return {
      icon: 'robot',
      copy: 'Unmark as bot',
      value: false,
    };
  }

  return {
    icon: 'robot',
    copy: 'Mark as bot',
    value: true,
  };
});

const handleMergeMembers = async () => {
  const [firstMember, secondMember] = selectedMembers.value;

  const { loadingMessage, apiErrorMessage } = useMemberMergeMessage;

  loadingMessage();

  return MemberService.merge(firstMember, secondMember)
    .then(() => {
      ToastStore.closeAll();
      ToastStore.info(
        "We're finalizing profiles merging. We will let you know once the process is completed.",
        {
          title: 'Profiles merging in progress',
        },
      );
    })
    .catch((error) => {
      apiErrorMessage({ error });
    });
};

const doDestroyAllWithConfirm = () => ConfirmDialog({
  type: 'danger',
  title: 'Delete profile',
  message:
        "Are you sure you want to proceed? You can't undo this action",
  confirmButtonText: 'Confirm',
  cancelButtonText: 'Cancel',
  icon: 'fa-trash-can fa-light',
})
  .then(() => {
    trackEvent({
      key: FeatureEventKey.DELETE_MEMBER,
      type: EventType.FEATURE,
      properties: {
        path: route.path,
        bulk: true,
      },
    });

    const ids = selectedMembers.value.map((m) => m.id);
    return MemberService.destroyAll(ids);
  })
  .then(async () => {
    // Clear selection immediately to prevent UI issues
    selectedMembers.value = [];

    // Refresh data to ensure UI is up to date
    await refreshMemberData();
  });

const handleDoExport = async () => {
  const ids = selectedMembers.value.map((i) => i.id);

  const filter = {
    id: {
      in: ids,
    },
  };

  try {
    await showExportDialog({
      badgeContent: pluralize('person', selectedMembers.value.length, true),
    });

    trackEvent({
      key: FeatureEventKey.EXPORT_MEMBERS,
      type: EventType.FEATURE,
      properties: {
        path: route.path,
        bulk: true,
      },
    });

    await MemberService.export({
      filter,
      orderBy: `${filters.value.order.prop}_${filters.value.order.order === 'descending' ? 'DESC' : 'ASC'}`,
      limit: 0,
      offset: null,
    });

    await getUser();

    ToastStore.success(
      'CSV download link will be sent to your e-mail',
    );
  } catch (error) {
    console.error(error);

    if (error !== 'cancel') {
      ToastStore.error(
        'An error has occured while trying to export the CSV file. Please try again',
        {
          title: 'CSV Export failed',
        },
      );
    }
  }
};

const handleEditAttribute = async () => {
  bulkAttributesUpdateVisible.value = true;
};

const doMarkAsTeamMember = async (value) => {
  ToastStore.info('People are being updated');

  const updatePromises = selectedMembers.value.map(async (member) => {
    // Fetch member with all attributes to prevent data loss
    const memberWithAllAttributes = await fetchMemberWithAllAttributes(member.id);
    const currentAttributes = memberWithAllAttributes.attributes;

    const updatedAttributes = {
      ...currentAttributes,
      isTeamMember: {
        default: value,
        custom: value,
      },
    };

    return MemberService.update(member.id, {
      attributes: updatedAttributes,
      invalidateCache: true,
    });
  });

  return Promise.all(updatePromises)
    .then(async () => {
      ToastStore.closeAll();
      ToastStore.success(`${
        pluralize('Person', selectedMembers.value.length, true)} updated successfully`);

      // Clear selection immediately to prevent UI issues
      selectedMembers.value = [];

      // Refresh data to ensure UI is up to date
      await refreshMemberData();
    })
    .catch(() => {
      ToastStore.closeAll();
      ToastStore.error('Error updating people');
    });
};

const doMarkAsBot = async (value) => {
  ToastStore.info('People are being updated');

  const updatePromises = selectedMembers.value.map(async (member) => {
    // Fetch member with all attributes to prevent data loss
    const memberWithAllAttributes = await fetchMemberWithAllAttributes(member.id);
    const currentAttributes = memberWithAllAttributes.attributes;

    const updatedAttributes = {
      ...currentAttributes,
      isBot: {
        ...currentAttributes.isBot,
        default: value,
        custom: value,
      },
    };

    return MemberService.update(member.id, {
      attributes: updatedAttributes,
      invalidateCache: true,
    });
  });

  return Promise.all(updatePromises)
    .then(async () => {
      ToastStore.closeAll();
      ToastStore.success(`${
        pluralize('Person', selectedMembers.value.length, true)} updated successfully`);

      // Clear selection immediately to prevent UI issues
      selectedMembers.value = [];

      // Refresh data to ensure UI is up to date
      await refreshMemberData();
    })
    .catch(() => {
      ToastStore.closeAll();
      ToastStore.error('Error updating people');
    });
};

const handleCommand = async (command) => {
  if (command.action === 'markAsTeamMember') {
    trackEvent({
      key: FeatureEventKey.MARK_AS_TEAM_MEMBER,
      type: EventType.FEATURE,
      properties: {
        path: route.path,
        bulk: true,
      },
    });

    await doMarkAsTeamMember(command.value);
  } else if (command.action === 'markAsBot') {
    trackEvent({
      key: FeatureEventKey.MARK_AS_BOT,
      type: EventType.FEATURE,
      properties: {
        path: route.path,
        bulk: true,
      },
    });

    await doMarkAsBot(command.value);
  } else if (command.action === 'export') {
    await handleDoExport();
  } else if (command.action === 'mergeMembers') {
    await handleMergeMembers();
  } else if (command.action === 'editAttribute') {
    await handleEditAttribute();
  } else if (command.action === 'destroyAll') {
    await doDestroyAllWithConfirm();
  }
};
</script>

<script>
export default {
  name: 'AppMemberListToolbar',
};
</script>
