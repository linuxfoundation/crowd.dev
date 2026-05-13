<template>
  <aside
    class="l-menu"
    :class="isCollapsed ? 'w-16 min-w-16' : 'w-65 min-w-65'"
  >
    <lf-menu-link
      title="Overview"
      icon="gauge-high"
      :is-collapsed="isCollapsed"
      :to="{ path: '/overview' }"
    />
    <div class="px-3 py-4">
      <app-lf-menu-project-group-selection
        v-if="!isCollapsed"
      />
      <el-divider v-else class="!mt-1 !mb-0" />
    </div>
    <lf-menu-link
      title="People"
      icon="user-group-simple"
      :is-collapsed="isCollapsed"
      :to="{ path: '/people', query: { projectGroup: selectedProjectGroup?.id } }"
      :disabled="!selectedProjectGroup"
      @click="onMembersClick"
    />
    <lf-menu-link
      title="Organizations"
      icon="building"
      :is-collapsed="isCollapsed"
      :to="{ path: '/organizations', query: { projectGroup: selectedProjectGroup?.id } }"
      :disabled="!selectedProjectGroup"
      @click="onOrganizationsClick"
    />
    <lf-menu-link
      title="Activities"
      icon="monitor-waveform"
      :is-collapsed="isCollapsed"
      :to="{ path: '/activities', query: { projectGroup: selectedProjectGroup?.id } }"
      :disabled="!selectedProjectGroup"
    />
    <template v-if="hasPermission(LfPermission.dataQualityRead)">
      <div class="px-3 py-2">
        <div class="border-b border-gray-200" />
      </div>
      <lf-menu-link

        title="Data Quality Copilot"
        icon="message-exclamation"
        :is-collapsed="isCollapsed"
        :to="{ path: '/data-quality-assistant', query: { projectGroup: selectedProjectGroup?.id } }"
        :disabled="!selectedProjectGroup"
      />
    </template>

    <div class="flex-grow" />
    <div class="flex justify-end pb-8">
      <div
        class="bg-gray-500 w-6 h-8 rounded-l flex items-center justify-center cursor-pointer"
        @click="toggleMenu()"
      >
        <lf-icon
          :name="`chevrons-${isCollapsed ? 'right' : 'left'}`"
          :size="16"
          class="text-white"
        />
      </div>
    </div>
    <template
      v-if="hasPermission(LfPermission.projectGroupCreate) || hasPermission(LfPermission.projectGroupEdit)"
    >
      <div class="px-3 py-2">
        <div class="border-b border-gray-200" />
      </div>
      <lf-menu-link
        title="Admin panel"
        icon="gear"
        :is-collapsed="isCollapsed"
        :to="{ path: '/admin' }"
      />
    </template>
  </aside>
</template>

<script setup>
import { watch } from 'vue';
import { useActivityTypeStore } from '@/modules/activity/store/type';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { storeToRefs } from 'pinia';
import AppLfMenuProjectGroupSelection from '@/modules/lf/layout/components/lf-menu-project-group-selection.vue';
import { mapGetters } from '@/shared/vuex/vuex.helpers';
import { useStore } from 'vuex';
import { useActivityStore } from '@/modules/activity/store/pinia';
import { useMemberStore } from '@/modules/member/store/pinia';
import allMembers from '@/modules/member/config/saved-views/views/all-members';
import allOrganizations from '@/modules/organization/config/saved-views/views/all-organizations';
import { useOrganizationStore } from '@/modules/organization/store/pinia';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import LfMenuLink from '@/modules/layout/components/menu/menu-link.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const store = useStore();

const { menuCollapsed: isCollapsed } = mapGetters('layout');

const { fetchActivityTypes } = useActivityTypeStore();
const { fetchActivityChannels } = useActivityStore();

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const memberStore = useMemberStore();
const { filters: membersFilters } = storeToRefs(memberStore);

const organizationStore = useOrganizationStore();
const { filters: organizationFilters } = storeToRefs(organizationStore);

const { hasPermission } = usePermissions();

const onMembersClick = () => {
  membersFilters.value = allMembers.config;
};

const onOrganizationsClick = () => {
  organizationFilters.value = allOrganizations.config;
};

watch(
  selectedProjectGroup,
  (updatedProjectGroup) => {
    if (updatedProjectGroup) {
      fetchActivityTypes();
      fetchActivityChannels();
    }
  },
  { immediate: true, deep: true },
);

function toggleMenu() {
  store.dispatch('layout/toggleMenu');
}

</script>

<script>
export default {
  name: 'LfMenu',
};
</script>
