<template>
  <!-- Unmerge -->
  <template v-if="organization.identities.length > 1 && !hideUnmerge && hasPermission(LfPermission.organizationEdit)">
    <button
      class="h-10 el-dropdown-menu__item w-full"
      type="button"
      @click="handleCommand({
        action: Actions.UNMERGE_IDENTITY,
        organization,
      })"
    >
      <lf-icon name="link-slash" :size="16" class="mr-2" />
      <span class="text-xs">Unmerge identity</span>
    </button>
    <el-divider class="border-gray-200 my-2" />
  </template>

  <!-- Edit -->
  <router-link
    v-if="!hideEdit && hasPermission(LfPermission.organizationEdit)"
    :to="{
      name: 'organizationEdit',
      params: {
        id: organization.id,
      },
      query: {
        segmentId: route.query.segmentId || route.query.projectGroup,
      },
    }"
  >
    <button
      class="h-10 el-dropdown-menu__item w-full"
      type="button"
    >
      <lf-icon name="pen fa-sharp" class="text-base mr-2" />
      <span class="text-xs">Edit organization</span>
    </button>
  </router-link>

  <!-- Merge organization -->
  <button
    v-if="!hideMerge && hasPermission(LfPermission.organizationEdit)"
    class="h-10 el-dropdown-menu__item w-full"
    type="button"
    :disabled="!hasPermission(LfPermission.mergeOrganizations)"
    @click="
      handleCommand({
        action: Actions.MERGE_ORGANIZATION,
        organization,
      })
    "
  >
    <lf-icon name="shuffle" :size="16" class="mr-2" />
    <span class="text-xs">Merge organization</span>
  </button>

  <!-- Mark as Team Organization -->
  <template v-if="hasPermission(LfPermission.organizationEdit)">
    <el-tooltip
      placement="top"
      content="Mark as team organization if it is your own organization."
      popper-class="max-w-[260px]"
    >
      <span>
        <button
          v-if="!organization.isTeamOrganization"
          class="h-10 el-dropdown-menu__item w-full"
          type="button"
          @click="
            handleCommand({
              action: Actions.MARK_ORGANIZATION_AS_TEAM_ORGANIZATION,
              organization,
              value: true,
            })
          "
        >
          <lf-icon name="bookmark" :size="16" class="mr-2" />
          <span class="text-xs">Mark as team organization</span>
        </button>
      </span>
    </el-tooltip>

    <!-- Unmark as Team Organization -->
    <button
      v-if="organization.isTeamOrganization"
      type="button"
      class="h-10 el-dropdown-menu__item w-full"
      @click="
        handleCommand({
          action: Actions.MARK_ORGANIZATION_AS_TEAM_ORGANIZATION,
          organization,
          value: false,
        })
      "
    >
      <lf-icon name="bookmark-slash" class="mr-2" /><span class="text-xs">Unmark as team organization</span>
    </button>
  </template>

  <!-- Block Organization Affiliations -->
  <template v-if="hasPermission(LfPermission.organizationEdit)">
    <button
      v-if="!organization.isAffiliationBlocked"
      class="h-10 el-dropdown-menu__item w-full"
      type="button"
      @click="
        handleCommand({
          action: Actions.TOGGLE_ORGANIZATION_AFFILIATIONS,
          organization,
          value: true,
        })
      "
    >
      <lf-icon name="ban" :size="16" class="mr-2" />
      <span class="text-xs">Block affiliations</span>
    </button>

    <button
      v-if="organization.isAffiliationBlocked"
      class="h-10 el-dropdown-menu__item w-full"
      type="button"
      @click="
        handleCommand({
          action: Actions.TOGGLE_ORGANIZATION_AFFILIATIONS,
          organization,
          value: false,
        })
      "
    >
      <lf-icon name="toggle-on" :size="16" class="mr-2" />
      <span class="text-xs">Enable affiliations</span>
    </button>
  </template>

  <template v-if="hasPermission(LfPermission.organizationDestroy)">
    <el-divider class="border-gray-200 my-2" />

    <!-- Delete -->
    <button
      class="h-10 el-dropdown-menu__item w-full"
      type="button"
      @click="
        handleCommand({
          action: Actions.DELETE_ORGANIZATION,
          organization,
        })
      "
    >
      <lf-icon name="trash-can" :size="16" class="mr-2 text-red-500" />
      <span
        class="text-xs text-red-500"
      >Delete organization</span>
    </button>
  </template>
</template>

<script setup lang="ts">
import { useRoute, useRouter } from 'vue-router';
import ConfirmDialog from '@/shared/dialog/confirm-dialog';
import { ToastStore } from '@/shared/message/notification';
import { useOrganizationStore } from '@/modules/organization/store/pinia';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { OrganizationService } from '../organization-service';
import { Organization } from '../types/Organization';

enum Actions {
  DELETE_ORGANIZATION = 'deleteOrganization',
  MERGE_ORGANIZATION = 'mergeOrganization',
  UNMERGE_IDENTITY = 'unmergeIdentity',
  SYNC_HUBSPOT = 'syncHubspot',
  STOP_SYNC_HUBSPOT = 'stopSyncHubspot',
  MARK_ORGANIZATION_AS_TEAM_ORGANIZATION = 'markOrganizationAsTeamOrganization',
  TOGGLE_ORGANIZATION_AFFILIATIONS = 'toggleOrganizationAffiliations',
}

const route = useRoute();
const router = useRouter();

const emit = defineEmits<{(e: 'merge'): void, (e: 'unmerge'): void, (e: 'closeDropdown'): void }>();
defineProps<{
  organization: Organization;
  hideMerge?: boolean;
  hideUnmerge?: boolean;
  hideEdit?: boolean;
}>();

const { trackEvent } = useProductTracking();

const organizationStore = useOrganizationStore();

const { hasPermission } = usePermissions();

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

const handleCommand = (command: {
  action: Actions;
  organization: Organization;
  value?: boolean;
}) => {
  // Delete organization
  if (command.action === Actions.DELETE_ORGANIZATION) {
    ConfirmDialog({
      type: 'danger',
      title: 'Delete organization',
      message: "Are you sure you want to proceed? You can't undo this action",
      confirmButtonText: 'Confirm',
      cancelButtonText: 'Cancel',
      icon: 'fa-trash-can fa-light',
    }).then(() => {
      trackEvent({
        key: FeatureEventKey.DELETE_ORGANIZATION,
        type: EventType.FEATURE,
        properties: {
          path: router.currentRoute.value.path,
        },
      });

      doManualAction({
        loadingMessage: 'Organization is being deleted',
        successMessage: 'Organization successfully deleted',
        errorMessage: 'Something went wrong',
        actionFn: OrganizationService.destroyAll([command.organization.id]),
      }).then(() => {
        organizationStore.fetchOrganizations({
          reload: true,
        });
      });
    });

    return;
  }

  // Merge organization
  if (command.action === Actions.MERGE_ORGANIZATION) {
    emit('closeDropdown');
    emit('merge');

    return;
  }

  // Unmerge identity
  if (command.action === Actions.UNMERGE_IDENTITY) {
    emit('closeDropdown');
    emit('unmerge');

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
        ? 'Organization is being synced with Hubspot'
        : 'Organization syncing with Hubspot is being stopped',
      successMessage: isSyncing
        ? 'Organization is now syncing with HubSpot'
        : 'Organization syncing stopped',
      errorMessage: 'Something went wrong',
      actionFn: isSyncing
        ? HubspotApiService.syncOrganization(command.organization.id)
        : HubspotApiService.stopSyncOrganization(command.organization.id),
    }).then(() => {
      if (router.currentRoute.value.name === 'organization') {
        organizationStore.fetchOrganizations({
          reload: true,
        });
      } else {
        organizationStore.fetchOrganization(command.organization.id, segments);
      }
    });

    return;
  }

  // Mark as team organization
  if (command.action === Actions.MARK_ORGANIZATION_AS_TEAM_ORGANIZATION) {
    trackEvent({
      key: FeatureEventKey.MARK_AS_TEAM_ORGANIZATION,
      type: EventType.FEATURE,
      properties: {
        path: router.currentRoute.value.path,
      },
    });

    doManualAction({
      loadingMessage: 'Organization is being updated',
      successMessage: 'Organization updated successfully',
      errorMessage: 'Something went wrong',
      actionFn: OrganizationService.update(command.organization.id, {
        isTeamOrganization: command.value,
      }, command.organization.segments),
    }).then(() => {
      if (router.currentRoute.value.name === 'organization') {
        organizationStore.fetchOrganizations({
          reload: true,
        });
      } else {
        organizationStore.fetchOrganization(command.organization.id, segments);
      }
    });

    return;
  }

  // Toggle organization affiliations
  if (command.action === Actions.TOGGLE_ORGANIZATION_AFFILIATIONS) {
    trackEvent({
      key: FeatureEventKey.TOGGLE_ORGANIZATION_AFFILIATIONS,
      type: EventType.FEATURE,
      properties: {
        path: router.currentRoute.value.path,
      },
    });

    doManualAction({
      loadingMessage: 'Organization is being updated',
      successMessage: 'Organization updated successfully',
      errorMessage: 'Something went wrong',
      actionFn: OrganizationService.update(command.organization.id, {
        isAffiliationBlocked: command.value,
      }, command.organization.segments),
    }).then(() => {
      if (router.currentRoute.value.name === 'organization') {
        organizationStore.fetchOrganizations({
          reload: true,
        });
      } else {
        organizationStore.fetchOrganization(command.organization.id, segments);
      }
    });

    return;
  }

  emit('closeDropdown');
};

</script>

<style lang="scss" scoped>
.el-dropdown-menu__item:disabled {
  @apply cursor-not-allowed text-gray-400;
}

.el-dropdown-menu__item:disabled:hover {
  @apply bg-white;
}
</style>
