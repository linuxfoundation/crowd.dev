<template>
  <div class="p-3">
    <div
      class="w-full flex items-center justify-between"
      :class="{ 'mb-9': !isProjectAdminUser }"
    >
      <h4 class="text-gray-900">
        Project Groups
      </h4>

      <div class="basis-1/4">
        <app-lf-search-input
          v-if="pagination.total && !isProjectAdminUser"
          placeholder="Search project group..."
          @on-change="onSearchProjectGroup"
        />
      </div>
    </div>

    <el-tabs v-if="isProjectAdminUser" v-model="computedActiveTab" class="mt-8">
      <el-tab-pane label="My project groups" name="project-groups" />
      <el-tab-pane label="All project groups" name="all-project-groups" />
    </el-tabs>

    <app-lf-search-input
      v-if="pagination.total && isProjectAdminUser"
      class="my-6"
      placeholder="Search project group..."
      @on-change="onSearchProjectGroup"
    />
    <div
      v-if="loading"
      v-loading="loading"
      class="app-page-spinner h-16 !relative !min-h-5 mt-10"
    />
    <div v-else>
      <app-empty-state-cta
        v-if="!pagination.total"
        class="mt-20"
        icon="folders"
        title="No project groups yet"
        :description="`${!hasPermission(LfPermission.projectGroupCreate)
          ? 'Ask an administrator to c' : 'C'}reate your first project group and start integrating your projects`"
        :cta-btn="hasPermission(LfPermission.projectGroupCreate) ? 'Manage project groups' : null"
        @cta-click="router.push({
          name: 'adminPanel',
          query: {
            activeTab: 'project-groups',
          },
        })"
      />

      <app-empty-state-cta
        v-else-if="!pagination.count"
        icon="folders"
        title="No project groups found"
        description="We couldn't find any results that match your search criteria, please try a different query"
      />

      <div v-else class="grid grid-cols-3 gap-5">
        <div
          v-for="projectGroup in list"
          :key="projectGroup.id"
          class="panel-card pb-6 flex flex-col"
        >
          <div
            class="min-h-32 h-32 flex items-center justify-center mb-6 px-6"
            style="background: linear-gradient(180deg, #FFFFFF 0%, #FAFAFA 100%), #FFFFFF;"
          >
            <img
              v-if="!imageErrors[projectGroup.id]"
              :src="projectGroup.url"
              :onerror="(evt) => handleImageError(projectGroup.id, evt)"
              alt="Project group logo"
              class="h-12 w-auto"
            />
            <lf-icon v-else name="image" :size="48" class="text-gray-200" />
          </div>

          <div class="px-6">
            <div class="text-gray-900 font-semibold text-base mb-5 break-words">
              {{ projectGroup.name }}
            </div>

            <div class="mb-8 flex flex-wrap gap-2.5">
              <div class="bg-gray-200 text-gray-900 text-2xs px-2 h-6 flex items-center w-fit rounded-md">
                {{ pluralize('project', projectGroup.projectCount ?? projectGroup.projects?.length ?? 0, true) }}
              </div>
            </div>

            <div class="flex grow" />

            <lf-button
              type="primary"
              size="medium"
              class="mb-4 w-full"
              @click="updateSelectedProjectGroup(projectGroup.id)"
            >
              View project(s)
            </lf-button>

            <router-link
              v-if="hasPermission(LfPermission.projectGroupEdit) && hasAccessToProjectGroup(projectGroup.id)"
              :to="{
                name: 'adminProjects',
                params: {
                  id: projectGroup.id,
                },
              }"
            >
              <lf-button
                type="secondary-gray"
                size="medium"
                class="w-full"
              >
                Settings
              </lf-button>
            </router-link>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { storeToRefs } from 'pinia';
import {
  computed, onMounted, reactive, ref, watch,
} from 'vue';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import AppLfSearchInput from '@/modules/admin/modules/projects/components/view/lf-search-input.vue';
import pluralize from 'pluralize';
import { useRoute, useRouter } from 'vue-router';
import { useAuthStore } from '@/modules/auth/store/auth.store';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import { LfRole } from '@/shared/modules/permissions/types/Roles';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';

const router = useRouter();
const route = useRoute();

const { trackEvent } = useProductTracking();

const authStore = useAuthStore();
const { roles } = storeToRefs(authStore);

const lsSegmentsStore = useLfSegmentsStore();
const { projectGroups } = storeToRefs(lsSegmentsStore);
const {
  listProjectGroups, updateSelectedProjectGroup, searchProjectGroup, listAdminProjectGroups,
} = lsSegmentsStore;

const { hasPermission, hasAccessToProjectGroup } = usePermissions();

const activeTab = ref();

const isProjectAdminUser = computed(() => roles.value.includes(LfRole.projectAdmin));

const adminOnly = computed(() => isProjectAdminUser.value && activeTab.value === 'project-groups');

const loadingProjectAdmin = ref(true);
const loading = computed(() => projectGroups.value.loading || loadingProjectAdmin.value);
const pagination = computed(() => projectGroups.value.pagination);
const list = computed(() => projectGroups.value.list);

const computedActiveTab = computed({
  get() {
    return activeTab.value;
  },
  set(value) {
    router.push({
      name: 'projectGroupsList',
      query: { activeTab: value },
    });
  },
});

const imageErrors = reactive({});

watch(() => route.query.activeTab, (newActiveTab) => {
  if (newActiveTab && isProjectAdminUser.value) {
    activeTab.value = newActiveTab;
    listProjectGroups({
      limit: null,
      offset: 0,
      adminOnly: adminOnly.value,
    });
  }
});

onMounted(() => {
  if (isProjectAdminUser.value) {
    activeTab.value = route.query.activeTab || 'project-groups';
  }

  listProjectGroups({
    limit: null,
    offset: 0,
    adminOnly: adminOnly.value,
  });
});

const handleImageError = (id, e) => {
  imageErrors[id] = true;
};

onMounted(() => {
  if (isProjectAdminUser.value) {
    listAdminProjectGroups().finally(() => {
      loadingProjectAdmin.value = false;
    });
  } else {
    loadingProjectAdmin.value = false;
  }
});

const onSearchProjectGroup = (query) => {
  trackEvent({
    key: FeatureEventKey.SEARCH_PROJECT_GROUPS,
    type: EventType.FEATURE,
  });

  searchProjectGroup(query, null, adminOnly.value);
};
</script>

<script>
export default {
  name: 'AppLfProjectGroupsListPage',
};
</script>
