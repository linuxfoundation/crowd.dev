<template>
  <el-input
    ref="inputRef"
    class="insights-projects-select-input"
    placeholder="Select project"
    readonly
    :suffix-icon="isPopoverVisible ? ArrowUpIcon : ArrowDownIcon"
    @click="openPopover"
  />

  <el-popover
    v-model:visible="isPopoverVisible"
    :virtual-ref="inputRef"
    placement="bottom"
    trigger="manual"
    virtual-triggering
    popper-class="insights-projects-select-popper"
    :teleported="false"
  >
    <div class="mb-2 border-b border-gray-100 px-2 pt-2 pb-1 w-full">
      <el-input
        id="filterSearch"
        v-model="searchQuery"
        placeholder="Search..."
        class="filter-dropdown-search"
        :prefix-icon="SearchIcon"
        @input="onSearchProjects(searchQuery)"
      />
    </div>

    <div class="p-2">
      <div v-if="displayProjects.length" class="flex flex-col gap-4">
        <div class="flex flex-col gap-1">
          <div
            v-for="project of displayProjects"
            :key="project.id"
            class="h-10 flex items-center px-3 text-xs text-gray-900 hover:bg-gray-50 rounded cursor-pointer"
            @click="onOptionClick(project)"
          >
            <lf-avatar
              :src="project.logoUrl"
              :name="project.name"
              :size="24"
              class="!rounded-md border border-gray-200 min-w-5"
            />
            <span class="ml-2 text-gray-900 text-sm line-clamp-2">{{ project.name }}</span>
          </div>
          <div
            v-if="isFetchingNextPage"
            class="text-gray-400 px-3 h-20 flex items-center justify-center"
          >
            <lf-icon name="circle-notch" class="animate-spin text-gray-400" :size="16" />
            <span class="text-tiny ml-1 text-gray-400">Loading projects...</span>
          </div>
        </div>
      </div>
      <div v-else-if="isPending" class="text-gray-400 px-3 h-20 flex items-center justify-center">
        <lf-icon name="circle-notch" class="animate-spin text-gray-400" :size="16" />
        <span class="text-tiny ml-1 text-gray-400">Loading projects...</span>
      </div>
      <div v-else class="text-gray-400 px-3 h-10 flex items-center">
        No projects found
      </div>
    </div>
  </el-popover>
</template>

<script setup lang="ts">
import {
  h, ref, computed, onMounted, nextTick, watch, onBeforeUnmount,
} from 'vue';
import type { InsightsProjectModel } from '@/modules/admin/modules/insights-projects/models/insights-project.model';
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import type { QueryFunction } from '@tanstack/vue-query';
import { useInfiniteQuery } from '@tanstack/vue-query';
import { useDebounce } from '@vueuse/core';
import type { Pagination } from '@/shared/types/Pagination';
import { TanstackKey } from '@/shared/types/tanstack';

import { ToastStore } from '@/shared/message/notification';
import { INSIGHTS_PROJECTS_SERVICE } from '../../insights-projects/services/insights-projects.service';
import { useInsightsProjectsStore } from '../../insights-projects/pinia';

const props = defineProps<{
  selectedProjects: InsightsProjectModel[];
}>();

const emit = defineEmits<{(e: 'onAddProject', projectId: string): void }>();

const SearchIcon = h(
  'i', // type
  { class: 'fa-light fa-magnifying-glass c-icon' }, // props
  [],
);

const ArrowDownIcon = h(
  'i', // type
  { class: 'fa-light fa-chevron-down c-icon' }, // props
  [],
);

const ArrowUpIcon = h(
  'i', // type
  { class: 'fa-light fa-chevron-up c-icon' }, // props
  [],
);

let scrollContainer: HTMLElement | null = null;

const insightsProjectsStore = useInsightsProjectsStore();

const inputRef = ref(null);
const searchQuery = ref('');
const searchValue = useDebounce(searchQuery, 300);
const isPopoverVisible = ref(false);
const displayProjects = computed(() => removeSelectedProject(
  insightsProjectsStore.searchInsightsProjects(searchQuery.value),
));
const queryKey = computed(() => [
  TanstackKey.ADMIN_INSIGHTS_PROJECTS,
  searchValue.value,
]);

const queryFn = INSIGHTS_PROJECTS_SERVICE.query(() => ({
  limit: 20,
  offset: 0,
  filter: searchValue.value
    ? {
      name: {
        like: `%${searchValue.value}%`,
      },
    }
    : {},
})) as QueryFunction<Pagination<InsightsProjectModel>, readonly unknown[], unknown>;

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
    const totalRows = lastPage.total!;
    return nextPage < totalRows ? nextPage : undefined;
  },
  initialPageParam: 0,
});

const removeSelectedProject = (projects: InsightsProjectModel[]) => {
  const selectedProjectsIds = props.selectedProjects.map(
    (project) => project.id,
  );
  return projects.filter(
    (project) => !selectedProjectsIds.includes(project.id),
  );
};

const onSearchProjects = (query: string) => {
  searchQuery.value = query;
};

const openPopover = () => {
  isPopoverVisible.value = true;
};

const onOptionClick = (project: InsightsProjectModel) => {
  isPopoverVisible.value = false;

  emit('onAddProject', project.id);
};

// Infinite scroll handler
function onScroll(e: Event) {
  if (!scrollContainer) return;
  const threshold = 20;

  const target = e.target as HTMLElement;
  if (
    !isFetchingNextPage.value
    && hasNextPage.value
    && target.scrollHeight - target.scrollTop - target.clientHeight < threshold
  ) {
    fetchNextPage();
  }
}

watch(data, () => {
  if (isSuccess.value && data.value) {
    let result = data.value.pages.reduce(
      (acc, page) => acc.concat(page.rows),
        [] as InsightsProjectModel[],
    );

    result = [...props.selectedProjects, ...result].reduce((acc, item) => {
      if (!acc.find((i) => i.id === item.id)) acc.push(item);
      return acc;
    }, [] as InsightsProjectModel[]);
    insightsProjectsStore.setInsightsProjects(result);
  }
}, { immediate: true });

watch(error, (err) => {
  if (err) {
    ToastStore.error('Something went wrong while fetching Insights projects');
  }
});

onMounted(() => {
  nextTick(() => {
    scrollContainer = document.querySelector(
      '.insights-projects-select-popper',
    );
    if (scrollContainer) {
      scrollContainer.addEventListener('scroll', onScroll);
    }
  });
});

onBeforeUnmount(() => {
  if (scrollContainer) {
    scrollContainer.removeEventListener('scroll', onScroll);
  }
});
</script>

<script lang="ts">
export default {
  name: 'LfInsightsProjectsListDropdown',
};
</script>

<style lang="scss">
.insights-projects-select-popper.el-popper {
  width: 100% !important;
  max-height: 480px;
  overflow: auto;
  @apply p-0;
}

.insights-projects-select-input {
  @apply cursor-pointer relative w-full;

  .el-input__inner {
    @apply cursor-pointer;
  }
}
</style>
