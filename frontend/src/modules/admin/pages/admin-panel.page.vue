<template>
  <app-page-wrapper>
    <div class="w-full flex justify-between">
      <h4 class="text-gray-900 py-6">
        Admin panel
      </h4>
      <div
        v-if="isAdminUser && isTeamUser"
        class="flex h-fit border border-gray-200 rounded-md p-1 gap-2"
      >
        <lf-button
          size="medium"
          class="!text-gray-500 text-xs !px-2"
          :class="{ '!bg-gray-100 !text-gray-900': isCommunityManagement }"
          type="secondary-ghost"
          @click="changeAdminPanelView('community')"
        >
          Community Data Platform
        </lf-button>
        <lf-button
          size="medium"
          class="!text-gray-500 text-xs !px-2"
          :class="{ '!bg-gray-100 !text-gray-900': !isCommunityManagement }"
          type="secondary-ghost"
          @click="changeAdminPanelView('insights')"
        >
          Insights
        </lf-button>
      </div>
    </div>
    <div>
      <lf-tabs v-model="activeTab" @update:model-value="changeView">
        <template v-if="isCommunityManagement">
          <lf-tab v-model="activeTab" name="project-groups">
            Projects
          </lf-tab>
          <template v-if="isAdminUser">
            <lf-tab v-model="activeTab" name="integrations">
              Integrations
            </lf-tab>
            <lf-tab v-model="activeTab" name="organizations">
              Organizations
            </lf-tab>
            <lf-tab v-model="activeTab" name="api-keys">
              API Keys
            </lf-tab>
            <lf-tab v-model="activeTab" name="audit-logs">
              Audit logs
            </lf-tab>
            <lf-tab v-model="activeTab" name="users">
              Users
            </lf-tab>
          </template>
        </template>
        <template v-else>
          <lf-tab v-model="activeTab" name="collections">
            Collections
          </lf-tab>
          <lf-tab v-model="activeTab" name="projects">
            Insights Projects
          </lf-tab>
          <lf-tab v-model="activeTab" name="categories">
            Categories
          </lf-tab>
        </template>
      </lf-tabs>
      <div class="mt-6 border-t border-gray-100">
        <div class="tab-content">
          <app-lf-project-groups-page v-if="activeTab === 'project-groups'" />
          <lf-admin-integration-status
            v-else-if="activeTab === 'integrations'"
          />
          <app-organization-common-page
            v-else-if="activeTab === 'organizations'"
          />
          <app-api-keys-page v-else-if="activeTab === 'api-keys'" />
          <app-lf-audit-logs-page v-else-if="activeTab === 'audit-logs'" />
          <lf-admin-users v-else-if="activeTab === 'users'" />
          <lf-collections-page v-else-if="activeTab === 'collections'" />
          <lf-insights-projects-page v-else-if="activeTab === 'projects'" />
          <lf-categories-page v-else-if="activeTab === 'categories'" />
        </div>
      </div>
    </div>
  </app-page-wrapper>
</template>

<script setup lang="ts">
import {
  computed, onMounted, ref, watch,
} from 'vue';
import { useRoute, useRouter } from 'vue-router';
import AppLfProjectGroupsPage from '@/modules/admin/modules/projects/pages/project-groups.page.vue';
import AppApiKeysPage from '@/modules/settings/pages/api-keys-page.vue';
import { useAuthStore } from '@/modules/auth/store/auth.store';
import { storeToRefs } from 'pinia';
import AppLfAuditLogsPage from '@/modules/lf/segments/pages/lf-audit-logs-page.vue';
import { LfRole } from '@/shared/modules/permissions/types/Roles';
import AppOrganizationCommonPage from '@/modules/organization/pages/organization-common-page.vue';
import LfAdminIntegrationStatus from '@/modules/admin/modules/integration/pages/integration-status.page.vue';
import LfAdminUsers from '@/modules/admin/modules/users/pages/users.page.vue';
import LfTabs from '@/ui-kit/tabs/Tabs.vue';
import LfTab from '@/ui-kit/tabs/Tab.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfCollectionsPage from '@/modules/admin/modules/collections/pages/collection.page.vue';
import LfInsightsProjectsPage from '@/modules/admin/modules/insights-projects/pages/insights-projects.page.vue';
import config from '@/config';
import LfCategoriesPage from '@/modules/admin/modules/categories/pages/categories.page.vue';
import { lfIntegrationStatusesTabs } from '@/modules/admin/modules/integration/config/status';

const route = useRoute();
const router = useRouter();

const activeTab = ref<string>();
const isCommunityManagement = ref(true);

const authStore = useAuthStore();
const { roles } = storeToRefs(authStore);

const changeView = (view: string) => {
  router.push({
    hash: `#${view}`,
    query: {},
  });
};

const isAdminUser = computed(() => roles.value.includes(LfRole.admin));

const userId = computed(() => authStore.user?.id);
const teamUserIds = computed(() => config.permissions.teamUserIds);
const env = computed(() => config.env);

const isTeamUser = computed(() => env.value !== 'production' || teamUserIds.value?.includes(userId.value!));

const changeAdminPanelView = (view: string) => {
  switch (view) {
    case 'community':
      isCommunityManagement.value = true;
      activeTab.value = 'project-groups';
      changeView('project-groups');

      break;
    default:
      isCommunityManagement.value = false;
      activeTab.value = 'collections';
      changeView('collections');
      break;
  }
};

onMounted(() => {
  const initialActiveTab = route.hash.substring(1) as string;

  if (
    (initialActiveTab === 'api-keys' || initialActiveTab === 'audit-logs')
    && !isAdminUser.value
  ) {
    activeTab.value = 'project-groups';
  } else if (
    initialActiveTab === 'collections'
    || initialActiveTab === 'projects'
    || initialActiveTab === 'categories'
  ) {
    activeTab.value = isAdminUser.value ? initialActiveTab : 'project-groups';
    isCommunityManagement.value = !isAdminUser.value;
  } else {
    activeTab.value = (route.hash.substring(1) as string) || 'project-groups';
  }
});

watch(
  () => route.hash,
  (hash: string) => {
    const view = hash.substring(1);
    if (view.length > 0 && view !== activeTab.value) {
      activeTab.value = view;
    }
  },
  { immediate: true },
);

watch(activeTab, (newVal) => {
  if (newVal === 'integrations') {
    if (window && window.localStorage) {
      window.localStorage.setItem('integrationStatusFilter', Object.keys(lfIntegrationStatusesTabs)[0]);
    }
  }
});
</script>
