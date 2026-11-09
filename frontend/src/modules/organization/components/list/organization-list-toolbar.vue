<template>
  <lf-table-bulk-actions :selected-items="selectedOrganizations">
    <el-dropdown trigger="click" @command="handleCommand">
      <lf-button type="secondary-gray" size="small">
        <span class="mr-2">Actions</span>
        <lf-icon name="chevron-down" :size="24" />
      </lf-button>
      <template #dropdown>
        <el-tooltip
          v-if="selectedOrganizations.length === 2 && hasPermission(LfPermission.mergeOrganizations)"
          content="Active member organizations of the Linux Foundation can't be merged into other organizations."
          :disabled="!(!!selectedOrganizations?.[0]?.lfxMembership && !!selectedOrganizations?.[1]?.lfxMembership)"
          placement="top"
        >
          <span>
            <el-dropdown-item
              :command="{
                action: 'mergeOrganizations',
              }"
              :disabled="
                (!!selectedOrganizations?.[0]?.lfxMembership && !!selectedOrganizations?.[1]?.lfxMembership)
                  || !hasPermission(LfPermission.organizationEdit)
                  || !hasPermission(LfPermission.mergeOrganizations)
              "
            >
              <lf-icon name="shuffling" :size="20" class="mr-1" />
              Merge organizations
            </el-dropdown-item>
          </span>
        </el-tooltip>

        <el-dropdown-item
          v-if="markAsTeamOrganizationOptions && hasPermission(LfPermission.organizationEdit)"
          :command="{
            action: 'markAsTeamOrganization',
            value: markAsTeamOrganizationOptions.value,
          }"
        >
          <lf-icon :name="markAsTeamOrganizationOptions.icon" :size="20" class="mr-1" />
          {{ markAsTeamOrganizationOptions.copy }}
        </el-dropdown-item>

        <template v-if="hasPermission(LfPermission.organizationDestroy)">
          <hr class="border-gray-200 my-1 mx-2" />

          <el-dropdown-item
            :command="{ action: 'destroyAll' }"
            :disabled="!hasPermission(LfPermission.organizationDestroy)"
          >
            <div
              class="flex items-center text-red-500"
            >
              <lf-icon name="trash-can" :size="20" class="mr-2" />
              <span>Delete organizations</span>
            </div>
          </el-dropdown-item>
        </template>
      </template>
    </el-dropdown>
  </lf-table-bulk-actions>
  <app-organization-merge-dialog v-model="isMergeDialogOpen" :to-merge="organizationToMerge" />
</template>

<script setup lang="ts">
import pluralize from 'pluralize';
import { ref, computed } from 'vue';
import { useRoute } from 'vue-router';
import { storeToRefs } from 'pinia';
import ConfirmDialog from '@/shared/dialog/confirm-dialog';

import { ToastStore } from '@/shared/message/notification';
import { useOrganizationStore } from '@/modules/organization/store/pinia';
import { useInvalidate } from '@/shared/queries/invalidate';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import type { Organization } from '@/modules/organization/types/Organization';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import AppOrganizationMergeDialog from '@/modules/organization/components/organization-merge-dialog.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfTableBulkActions from '@/ui-kit/table/table-bulk-actions.vue';
import { OrganizationService } from '@/modules/organization/services/organization.api.service';

defineOptions({ name: 'AppOrganizationListToolbar' });

const { trackEvent } = useProductTracking();

const route = useRoute();

const organizationStore = useOrganizationStore();
const {
  selectedOrganizations,
  filters,
} = storeToRefs(organizationStore);
const invalidate = useInvalidate();

const reloadOrganizations = () => {
  selectedOrganizations.value = [];
  return invalidate.organizations();
};

const { hasPermission } = usePermissions();

const isMergeDialogOpen = ref<Organization | null>(null);
const organizationToMerge = ref<Organization | null>(null);

const markAsTeamOrganizationOptions = computed(() => {
  // shortcut: keeps main's throw without settings. revisit: when Filter.settings is required
  const isTeamView = filters.value.settings!.teamOrganization === 'filter';
  const organizationsCopy = pluralize(
    'organization',
    selectedOrganizations.value.length,
    false,
  );

  if (isTeamView) {
    return {
      icon: 'bookmark-slash',
      copy: `Unmark as team ${organizationsCopy}`,
      value: false,
    };
  }

  return {
    icon: 'bookmark',
    copy: `Mark as team ${organizationsCopy}`,
    value: true,
  };
});

const handleDoDestroyAllWithConfirm = () => ConfirmDialog({
  type: 'danger',
  title: 'Delete organizations',
  message:
      "Are you sure you want to proceed? You can't undo this action",
  confirmButtonText: 'Confirm',
  cancelButtonText: 'Cancel',
  icon: 'fa-trash-can fa-light',
})
  .then(() => {
    trackEvent({
      key: FeatureEventKey.DELETE_ORGANIZATION,
      type: EventType.FEATURE,
      properties: {
        path: route.path,
        bulk: true,
      },
    });

    const ids = selectedOrganizations.value.map((m) => m.id);
    return OrganizationService.destroyAll(ids);
  })
  .then(() => reloadOrganizations());

const handleMergeOrganizations = async () => {
  const [firstOrganization, secondOrganization] = selectedOrganizations.value;

  isMergeDialogOpen.value = firstOrganization;
  organizationToMerge.value = secondOrganization;
};

type OrganizationListToolbarCommand =
  | { action: 'destroyAll' }
  | { action: 'mergeOrganizations' }
  | { action: 'markAsTeamOrganization'; value: boolean };

const handleCommand = async (command: OrganizationListToolbarCommand) => {
  if (command.action === 'destroyAll') {
    await handleDoDestroyAllWithConfirm();
  } else if (command.action === 'mergeOrganizations') {
    await handleMergeOrganizations();
  } else if (command.action === 'markAsTeamOrganization') {
    trackEvent({
      key: FeatureEventKey.MARK_AS_TEAM_ORGANIZATION,
      type: EventType.FEATURE,
      properties: {
        path: route.path,
        bulk: true,
      },
    });

    ToastStore.info('Organizations are being updated');

    Promise.all(
      selectedOrganizations.value.map((row) => OrganizationService.update(row.id, {
        isTeamOrganization: command.value,
      })),
    ).then(() => {
      ToastStore.closeAll();
      ToastStore.success(
        `${pluralize(
          'Organization',
          selectedOrganizations.value.length,
          false,
        )} updated successfully`,
      );

      reloadOrganizations();
    })
      .catch(() => {
        ToastStore.closeAll();
        ToastStore.error('Error updating organizations');
      });
  }
};
</script>
