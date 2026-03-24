<template>
  <div v-if="organizations.length">
    <lf-table class="pb-12">
      <thead>
        <tr>
          <lf-table-head v-model="sort" property="displayName" class="pl-3">
            Organization
          </lf-table-head>
          <lf-table-head>
            Project groups
          </lf-table-head>
          <lf-table-head>
            Identities
          </lf-table-head>
          <lf-table-head />
        </tr>
      </thead>

      <tbody>
        <router-link
          v-for="(org, oi) of props.organizations"
          :key="org.id"
          class="table-row tr hover:bg-gray-100 transition h-16 max-h-16"
          :to="{ name: 'organizationView', params: { id: org.id } }"
        >
          <lf-table-cell class="pl-3">
            <app-organization-name
              class="w-full"
              :organization="org"
            />
          </lf-table-cell>
          <lf-table-cell>
            <div class="flex py-0.5">
              <el-popover v-if="getSegments(org.segments).length > 0" placement="top-start" width="264px">
                <template #reference>
                  <div class="border border-gray-200 h-6 px-1.5 rounded-md bg-white text-small gap-1 flex items-center whitespace-nowrap text-black">
                    <lf-icon name="folders" :size="14" />
                    {{ pluralize('project group', getSegments(org.segments).length, true) }}
                  </div>
                </template>
                <div class="max-h-72 overflow-auto pb-1">
                  <div class="mb-2.5 text-tiny font-semibold text-gray-400">
                    Project groups
                  </div>
                  <div class="flex flex-col gap-4 min-h-min ">
                    <div
                      v-for="segment in getSegments(org.segments)"
                      :key="segment.id"
                      class="flex items-center gap-2"
                    >
                      <lf-icon name="folder" :size="16" class="text-gray-400" />
                      <span class="text-small text-black">{{ segment.name }}</span>
                    </div>
                  </div>
                </div>
              </el-popover>
              <div v-else class="text-black">
                -
              </div>
            </div>
          </lf-table-cell>
          <lf-table-cell>
            <div class="flex">
              <app-identities-horizontal-list-organizations
                :organization="org"
                :limit="5"
              />
            </div>
          </lf-table-cell>
          <lf-table-cell :style="{ 'z-index': props.organizations.length - oi + 2 }">
            <div @click.stop.prevent>
              <lf-dropdown placement="bottom-end" @click.stop.prevent>
                <template #trigger>
                  <lf-button type="secondary-ghost-light" :icon-only="true" size="small" @click.prevent>
                    <lf-icon name="ellipsis" :size="20" />
                  </lf-button>
                </template>
                <lf-dropdown-item
                  @click="markAsTeamOrganization(org)"
                >
                  <lf-icon name="people-group" />
                  {{ org.isTeamOrganization ? 'Unmark' : 'Mark' }} as team organization
                </lf-dropdown-item>
                <lf-dropdown-item @click="toggleOrganizationAffiliations(org)">
                  <lf-icon name="ban" />
                  {{ org.isAffiliationBlocked ? 'Enable' : 'Block' }} affiliations
                </lf-dropdown-item>
                <lf-dropdown-separator />
                <lf-dropdown-item type="danger" @click="deleteOrganization(org)">
                  <lf-icon name="trash-can" />
                  Delete organization
                </lf-dropdown-item>
              </lf-dropdown>
            </div>
          </lf-table-cell>
        </router-link>
      </tbody>

      <tfoot class="border-b border-gray-100" style="z-index: 0">
        <tr>
          <lf-table-cell class="py-4" :sticky="true" colspan="2" style="z-index: 0">
            <slot name="pagination" />
          </lf-table-cell>
        </tr>
      </tfoot>
      <div class="absolute bottom-0 left-0 right-0 bg-white h-12" />
    </lf-table>
  </div>
  <app-empty-state-cta
    v-else
    icon="house-building"
    title="No organizations found"
    description="We couldn't find any results that match your search criteria, please try a different query."
  />
</template>

<script setup lang="ts">
import LfTable from '@/ui-kit/table/Table.vue';
import LfTableHead from '@/ui-kit/table/TableHead.vue';
import AppOrganizationName from '@/modules/organization/components/organization-name.vue';
import AppIdentitiesHorizontalListOrganizations
  from '@/shared/modules/identities/components/identities-horizontal-list-organizations.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfDropdownSeparator from '@/ui-kit/dropdown/DropdownSeparator.vue';
import LfTableCell from '@/ui-kit/table/TableCell.vue';
import { computed } from 'vue';
import { OrganizationService } from '@/modules/organization/organization-service';

import { ToastStore } from '@/shared/message/notification';
import ConfirmDialog from '@/shared/dialog/confirm-dialog';
import pluralize from 'pluralize';
import { getSegmentName } from '@/utils/segments';
import AppEmptyStateCta from '@/shared/empty-state/empty-state-cta.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  sorting: string,
  organizations: any[]
}>();

const emit = defineEmits<{(e: 'reload'): void, (e: 'update:sorting', value: string): void}>();

const sort = computed<string>({
  get() {
    return props.sorting;
  },
  set(val: string) {
    emit('update:sorting', val);
  },
});

const markAsTeamOrganization = (organization: any) => {
  ToastStore.info('Organization is being updated');
  OrganizationService.update(organization.id, {
    isTeamOrganization: !organization.isTeamOrganization,
  }, organization.segments)
    .then(() => {
      ToastStore.closeAll();
      ToastStore.success('Organization updated successfully');
      emit('reload');
    })
    .catch(() => {
      ToastStore.closeAll();
      ToastStore.error('Something went wrong');
    });
};

const toggleOrganizationAffiliations = (organization: any) => {
  ToastStore.info('Organization is being updated');
  OrganizationService.update(organization.id, {
    isAffiliationBlocked: !organization.isAffiliationBlocked,
  }, organization.segments)
    .then(() => {
      ToastStore.closeAll();
      ToastStore.success('Organization updated successfully');
      emit('reload');
    })
    .catch(() => {
      ToastStore.closeAll();
      ToastStore.error('Something went wrong');
    });
};

const deleteOrganization = (organization: any) => {
  ConfirmDialog({
    type: 'danger',
    title: 'Delete organization',
    message: "Are you sure you want to proceed? You can't undo this action",
    confirmButtonText: 'Confirm',
    cancelButtonText: 'Cancel',
    icon: 'fa-trash-can fa-light',
  }).then(() => {
    ToastStore.info('Organization is being deleted');
    OrganizationService.destroyAll([organization.id], organization.segments)
      .then(() => {
        ToastStore.closeAll();
        ToastStore.success('Organization successfully deleted');
        emit('reload');
      })
      .catch(() => {
        ToastStore.closeAll();
        ToastStore.error('Something went wrong');
      });
  });
};

const getSegments = (segments: string[]): {id: string, name: string}[] => segments.map((segment) => ({
  id: segment,
  name: getSegmentName(segment) || '',
})).filter((s) => s.name.length > 0);
</script>

<script lang="ts">
export default {
  name: 'AppOrganizationComonListTable',
};
</script>

<style lang="scss">
.table-row {
  vertical-align: inherit;
}
</style>
