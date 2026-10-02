<template>
  <div class="pt-6">
    <div class="flex gap-4">
      <!-- Search input -->
      <app-lf-search-input
        v-if="pagination.count || searchQuery.length"
        placeholder="Search project groups..."
        @on-change="onSearchProjectGroup"
      />
      <lf-button
        v-if="
          pagination.count && hasPermission(LfPermission.projectGroupCreate)
        "
        size="medium"
        type="secondary-ghost"
        @click="onAddProjectGroup"
      >
        <lf-icon name="folder-plus" type="regular" />
        Add project group
      </lf-button>
    </div>

    <div
      v-if="isPending"
      v-loading="isPending"
      class="app-page-spinner h-16 !relative !min-h-5 mt-10"
    />
    <div v-else>
      <!-- Empty state -->
      <app-empty-state-cta
        v-if="!pagination.count"
        class="mt-20"
        icon="folders"
        title="No project groups yet"
        :description="`${
          !hasPermission(LfPermission.projectGroupCreate)
            ? 'Ask an administrator to c'
            : 'C'
        }reate your first project group and start integrating your projects`"
        :cta-btn="
          hasPermission(LfPermission.projectGroupCreate)
            ? 'Add project group'
            : null
        "
        @cta-click="onAddProjectGroup"
      />

      <app-empty-state-cta
        v-else-if="!pagination.rows.length"
        icon="folders"
        title="No project groups found"
        description="We couldn't find any results that match your search criteria, please try a different query"
      />

      <!-- Table -->
      <div v-else class="mt-8">
        <app-lf-project-groups-table
          :pagination="pagination"
          :loading="isPending"
          :is-fetching-next-page="isFetchingNextPage"
          @on-edit-project-group="onEditProjectGroup"
          @on-add-project="onAddProject"
          @on-load-more="onLoadMore"
        />
      </div>
    </div>

    <app-lf-project-group-form
      v-if="isProjectGroupFormDrawerOpen"
      :id="projectGroupForm.id"
      v-model="isProjectGroupFormDrawerOpen"
    />

    <app-lf-project-form
      v-if="isProjectFormDrawerOpen"
      v-model="isProjectFormDrawerOpen"
      :parent-slug="projectGroupForm.parentSlug!"
    />
  </div>
</template>

<script setup lang="ts">
import { storeToRefs } from 'pinia';
import {
  computed, reactive, ref, watch,
} from 'vue';
import AppLfProjectGroupForm from '@/modules/admin/modules/projects/components/form/lf-project-group-form.vue';
import AppLfProjectForm from '@/modules/admin/modules/projects/components/form/lf-project-form.vue';
import AppLfProjectGroupsTable from '@/modules/admin/modules/projects/components/view/lf-project-groups-table.vue';
import AppLfSearchInput from '@/modules/admin/modules/projects/components/view/lf-search-input.vue';
import { useAuthStore } from '@/modules/auth/store/auth.store';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import { LfRole } from '@/shared/modules/permissions/types/Roles';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import {
  EventType,
  FeatureEventKey,
} from '@/shared/modules/monitoring/types/event';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import type { QueryFunction } from '@tanstack/vue-query';
import { useInfiniteQuery } from '@tanstack/vue-query';
import type { Pagination } from '@/shared/types/Pagination';
import type { ProjectGroup } from '@/modules/lf/segments/types/Segments';
import { TanstackKey } from '@/shared/types/tanstack';
import { segmentService } from '@/modules/lf/segments/segments.service';

import { ToastStore } from '@/shared/message/notification';

const authStore = useAuthStore();
const { roles } = storeToRefs(authStore);

const { trackEvent } = useProductTracking();
const { hasPermission } = usePermissions();

const searchQuery = ref('');

const projectGroupForm = reactive<{
  id: string | null;
  parentSlug: string | null;
}>({
  id: null,
  parentSlug: null,
});
const isProjectGroupFormDrawerOpen = ref(false);
const isProjectFormDrawerOpen = ref(false);

const isProjectAdminUser = computed(() => roles.value.includes(LfRole.projectAdmin));

const queryKey = computed(() => [
  TanstackKey.ADMIN_PROJECT_GROUPS,
  isProjectAdminUser.value,
  searchQuery.value,
]);

const projectGroupsQueryFn = segmentService.queryProjectGroups(() => ({
  limit: 20,
  offset: 0,
  filter: {
    name: searchQuery.value,
    adminOnly: isProjectAdminUser.value,
  },
})) as QueryFunction<Pagination<ProjectGroup>, readonly unknown[], unknown>;

const {
  data,
  isPending,
  isFetchingNextPage,
  fetchNextPage,
  hasNextPage,
  isSuccess,
  error,
} = useInfiniteQuery<Pagination<ProjectGroup>, Error>({
  queryKey,
  queryFn: projectGroupsQueryFn,
  getNextPageParam: (lastPage) => {
    const nextPage = lastPage.offset + lastPage.limit;
    const totalRows = lastPage.count;
    return nextPage < totalRows ? nextPage : undefined;
  },
  initialPageParam: 0,
});

const pagination = computed((): Pagination<ProjectGroup> => {
  if (isSuccess.value && data.value) {
    return {
      count: data.value.pages[0].count,
      limit: data.value.pages[0].limit,
      offset: data.value.pages[0].offset,
      rows: data.value.pages.reduce(
        (acc, page) => acc.concat(page.rows),
        [] as ProjectGroup[],
      ),
    };
  }
  return {
    count: 0,
    limit: 20,
    offset: 0,
    rows: [],
  };
});

watch(error, (err) => {
  if (err) {
    ToastStore.error('Something went wrong while fetching project groups');
  }
});

const onLoadMore = () => {
  if (hasNextPage.value && !isFetchingNextPage.value && !isPending.value) {
    fetchNextPage();
  }
};

const onAddProject = (parentSlug: string) => {
  projectGroupForm.parentSlug = parentSlug;
  isProjectFormDrawerOpen.value = true;
};

const onAddProjectGroup = () => {
  projectGroupForm.id = null;
  isProjectGroupFormDrawerOpen.value = true;
};

const onEditProjectGroup = (id: string) => {
  projectGroupForm.id = id;
  isProjectGroupFormDrawerOpen.value = true;
};

const onSearchProjectGroup = (val: string) => {
  trackEvent({
    key: FeatureEventKey.SEARCH_PROJECT_GROUPS,
    type: EventType.FEATURE,
  });

  searchQuery.value = val;
};
</script>

<script lang="ts">
export default {
  name: 'AppLfProjectGroupsPage',
};
</script>
