<template>
  <div>
    <div
      v-if="projects.length > 0 || search.length > 0"
      class="flex gap-4 pt-6 pb-4"
    >
      <!-- Search input -->
      <lf-search
        v-model="search"
        class="h-9 flex-grow"
        :lazy="true"
        placeholder="Search projects..."
      />
      <lf-button
        size="medium"
        type="secondary-ghost"
        @click="openInsightsProjectAdd"
      >
        <lf-icon name="plus" :size="16" />
        Add project
      </lf-button>
    </div>
    <div v-if="!isPending && projects.length > 0">
      <lf-insights-projects-table
        :projects="projects"
        @on-edit-project="onEditInsightsProject($event)"
        @on-delete-project="onDeleteProject($event)"
      />
      <div class="pt-4">
        <lf-button
          v-if="hasNextPage && !isFetchingNextPage"
          type="primary-ghost"
          @click="fetchNextPage()"
        >
          Load more
        </lf-button>
        <div
          v-else-if="isFetchingNextPage"
          class="flex items-center justify-center"
        >
          <span class="text-xs text-gray-400 mr-4">
            {{ offset }} out of {{ total }} projects
          </span>
          <div class="flex items-center text-xs text-primary-200">
            <lf-spinner :size="'1rem'" class="mr-1 border-primary-200" />
            Loading projects...
          </div>
        </div>
      </div>
    </div>

    <div v-else-if="!isPending" class="flex flex-col items-center">
      <app-empty-state-cta
        v-if="searchValue.length"
        class="w-full !pb-0"
        icon="laptop-code"
        :icon-size="100"
        title="No projects found"
        description="We couldn't find any results that match your search criteria, please try a different query"
      />
      <template v-else>
        <app-empty-state-cta
          class="w-full !pb-0"
          icon="laptop-code"
          :icon-size="100"
          title="No projects yet"
          description="Add projects to Insights and organize them within collections"
        />
        <lf-button
          class="w-fit"
          size="medium"
          type="primary-ghost"
          @click="openInsightsProjectAdd"
        >
          <lf-icon name="plus" :size="16" />
          Add project
        </lf-button>
      </template>
    </div>
    <div v-else class="pt-8 flex justify-center">
      <lf-spinner />
    </div>
  </div>

  <lf-insights-project-add
    v-if="isProjectDialogOpen"
    v-model="isProjectDialogOpen"
    :insights-project-id="selectedProject?.id"
    @on-insights-project-created="onInsightsProjectDialogClose"
    @on-insights-project-edited="onInsightsProjectDialogClose"
    @update:model-value="onInsightsProjectDialogClose"
  />

  <app-delete-confirm-dialog
    v-if="removeProjectDialog"
    v-model="removeProjectDialog"
    title="Are you sure you want to remove this project from Insights?"
    description="This will remove the project permanently. You can't undo this action."
    icon="circle-minus"
    confirm-button-text="Remove project"
    cancel-button-text="Cancel"
    confirm-text="remove"
    @confirm="removeMutation.mutate(selectedProject!.id)"
    @close="onCloseRemoveProject"
  />
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import LfSearch from '@/ui-kit/search/Search.vue';

import { ToastStore } from '@/shared/message/notification';
import LfInsightsProjectsTable from '@/modules/admin/modules/insights-projects/components/lf-insights-projects-table.vue';
import AppEmptyStateCta from '@/shared/empty-state/empty-state-cta.vue';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import AppDeleteConfirmDialog from '@/shared/dialog/delete-confirm-dialog.vue';
import { cloneDeep } from 'lodash';
import { TanstackKey } from '@/shared/types/tanstack';
import type {
  QueryFunction,
} from '@tanstack/vue-query';
import {
  useInfiniteQuery,
  useMutation,
  useQueryClient,
} from '@tanstack/vue-query';
import { useDebounce } from '@vueuse/core';
import type { Pagination } from '@/shared/types/Pagination';
import { useRoute, useRouter } from 'vue-router';
import LfInsightsProjectAdd from '../components/lf-insights-project-add.vue';
import { INSIGHTS_PROJECTS_SERVICE } from '../services/insights-projects.service';
import type { InsightsProjectModel } from '../models/insights-project.model';

const queryClient = useQueryClient();

const search = ref('');
const searchValue = useDebounce(search, 300);

const offset = ref(0);
const limit = ref(20);

const isProjectDialogOpen = ref<boolean>(false);
const selectedProject = ref<InsightsProjectModel | undefined>(undefined);
const removeProjectDialog = ref<boolean>(false);

const queryKey = computed(() => [
  TanstackKey.ADMIN_INSIGHTS_PROJECTS,
  searchValue.value,
]);

const queryFn = INSIGHTS_PROJECTS_SERVICE.query(() => ({
  filter: searchValue.value
    ? {
      name: {
        like: `%${searchValue.value}%`,
      },
    }
    : {},
  offset: 0,
  limit: limit.value,
})) as QueryFunction<
  Pagination<InsightsProjectModel>,
  readonly unknown[],
  unknown
>;

const route = useRoute();
const router = useRouter();

const {
  data,
  isPending,
  isFetchingNextPage,
  fetchNextPage,
  hasNextPage,
  isSuccess,
  error,
} = useInfiniteQuery<Pagination<InsightsProjectModel>, Error>({
  queryKey,
  queryFn,
  getNextPageParam: (lastPage) => {
    const nextPage = lastPage.offset + lastPage.limit;
    const totalRows = lastPage.total || lastPage.count;
    return nextPage < totalRows ? nextPage : undefined;
  },
  initialPageParam: 0,
});

const projects = computed((): InsightsProjectModel[] => {
  if (isSuccess.value && data.value) {
    return data.value.pages.reduce(
      (acc, page) => acc.concat(page.rows),
      [] as InsightsProjectModel[],
    );
  }
  return [];
});

watch(() => route.query, (query) => {
  if (query?.search !== search.value) {
    search.value = query.search as string || '';
  }
}, {
  immediate: true,
});

watch(search, (value) => {
  if (value !== route.query?.search) {
    router.replace({
      query: {
        ...route.query,
        search: value || undefined,
      },
    });
  }
});

watch(error, (err) => {
  if (err) {
    ToastStore.error('Something went wrong while fetching projects');
  }
});

const total = computed((): number => {
  if (isSuccess.value && data.value) {
    return data.value.pages[0].total || data.value.pages[0].count;
  }
  return 0;
});

const removeMutation = useMutation({
  mutationFn: (projectId: string) => INSIGHTS_PROJECTS_SERVICE.delete(projectId),
  onSuccess: () => {
    ToastStore.closeAll();
    ToastStore.success('Project successfully removed');
    queryClient.invalidateQueries({
      queryKey: [TanstackKey.ADMIN_INSIGHTS_PROJECTS],
    });
    onCloseRemoveProject();
  },
  onError: () => {
    ToastStore.closeAll();
    ToastStore.error('Something went wrong');
    onCloseRemoveProject();
  },
  onMutate: () => {
    ToastStore.info('Project is being removing');
  },
});

const openInsightsProjectAdd = () => {
  isProjectDialogOpen.value = true;
  selectedProject.value = undefined;
};

const onEditInsightsProject = (projectId: string) => {
  isProjectDialogOpen.value = true;
  updateSelectedProject(projectId);
};

const onInsightsProjectDialogClose = () => {
  isProjectDialogOpen.value = false;
  selectedProject.value = undefined;
};

const onDeleteProject = (projectId: string) => {
  removeProjectDialog.value = true;
  updateSelectedProject(projectId);
};

const onCloseRemoveProject = () => {
  removeProjectDialog.value = false;
  selectedProject.value = undefined;
};

const updateSelectedProject = (projectId: string) => {
  selectedProject.value = cloneDeep(
    projects.value.find((project) => project.id === projectId),
  );
};

</script>

<script lang="ts">
export default {
  name: 'LfInsightsProjectsPage',
};
</script>
