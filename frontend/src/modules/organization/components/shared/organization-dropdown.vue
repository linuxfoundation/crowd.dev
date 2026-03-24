<template>
  <lf-dropdown-item
    v-if="hasSegments && hasPermission(LfPermission.organizationEdit) && props.organization.identities.length > 1"
    @click="emit('unmerge')"
  >
    <lf-icon name="link-simple-slash" />
    Unmerge identity
  </lf-dropdown-item>
  <lf-dropdown-item
    v-if="hasPermission(LfPermission.organizationEdit)"
    @click="markTeamOrganization(!props.organization.isTeamOrganization)"
  >
    <lf-icon name="people-group" />
    {{ props.organization.isTeamOrganization ? 'Unmark' : 'Mark' }} as team organization
  </lf-dropdown-item>
  <lf-dropdown-item
    v-if="hasPermission(LfPermission.organizationEdit)"
    @click="toggleOrganizationAffiliations(!props.organization.isAffiliationBlocked)"
  >
    <lf-icon name="ban" />
    {{ props.organization.isAffiliationBlocked ? 'Enable' : 'Block' }} affiliations
  </lf-dropdown-item>

  <template v-if="hasPermission(LfPermission.organizationDestroy)">
    <lf-dropdown-separator />
    <lf-dropdown-item type="danger" @click="deleteOrganization()">
      <lf-icon name="trash-can" />
      Delete organization
    </lf-dropdown-item>
  </template>
</template>

<script setup lang="ts">
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfDropdownSeparator from '@/ui-kit/dropdown/DropdownSeparator.vue';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { useRoute, useRouter } from 'vue-router';
import { doManualAction } from '@/shared/helpers/manualAction.helpers';
import ConfirmDialog from '@/shared/dialog/confirm-dialog';
import { Organization } from '@/modules/organization/types/Organization';
import { OrganizationService } from '@/modules/organization/organization-service';
import { useOrganizationStore } from '@/modules/organization/store/pinia';

const props = defineProps<{
  organization: Organization,
  hasSegments: boolean,
}>();

const emit = defineEmits<{(e: 'reload'): any, (e: 'unmerge'): void}>();

const route = useRoute();
const router = useRouter();
const { hasPermission } = usePermissions();
const { trackEvent } = useProductTracking();

const { fetchOrganization } = useOrganizationStore();

const markTeamOrganization = (teamOrganization: boolean) => {
  trackEvent({
    key: FeatureEventKey.MARK_AS_TEAM_ORGANIZATION,
    type: EventType.FEATURE,
    properties: {
      path: route.path,
      teamOrganization,
    },
  });

  doManualAction({
    loadingMessage: 'Organization is being updated',
    successMessage: 'Organization updated successfully',
    errorMessage: 'Something went wrong',
    actionFn: OrganizationService.update(props.organization.id, {
      isTeamOrganization: teamOrganization,
    }, props.organization.segments),
  }).then(() => {
    fetchOrganization(props.organization.id);
    emit('reload');
  });
};

const toggleOrganizationAffiliations = (isAffiliationBlocked: boolean) => {
  trackEvent({
    key: FeatureEventKey.TOGGLE_ORGANIZATION_AFFILIATIONS,
    type: EventType.FEATURE,
  });

  doManualAction({
    loadingMessage: 'Organization is being updated',
    successMessage: 'Organization updated successfully',
    errorMessage: 'Something went wrong',
    actionFn: OrganizationService.update(props.organization.id, {
      isAffiliationBlocked,
    }, props.organization.segments),
  }).then(() => {
    fetchOrganization(props.organization.id);
    emit('reload');
  });
};

const deleteOrganization = () => {
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
        path: route.path,
      },
    });

    doManualAction({
      loadingMessage: 'Organization is being deleted',
      successMessage: 'Organization successfully deleted',
      errorMessage: 'Something went wrong',
      actionFn: OrganizationService.destroyAll([props.organization.id]),
    }).then(() => {
      router.push({
        path: '/organizations',
        query: {
          projectGroup: route.query.projectGroup,
        },
      });
    });
  });
};
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDropdown',
};
</script>
