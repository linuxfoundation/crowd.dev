<template>
  <el-popover
    v-model:visible="isPopoverVisible"
    placement="bottom-start"
    trigger="click"
    popper-class="project-groups-select-popper"
    :teleported="false"
    width="255px"
  >
    <template #reference>
      <el-input
        v-model="model"
        class="project-groups-select-input"
        placeholder="Select project group..."
        readonly
        :suffix-icon="isPopoverVisible ? ArrowUpIcon : ArrowDownIcon"
      />
    </template>

    <div
      v-if="isSearchVisible"
      class="border-b border-gray-100 px-1 w-full sticky top-0 bg-white"
    >
      <el-input
        id="filterSearch"
        v-model="searchQuery"
        placeholder="Search..."
        class="filter-dropdown-search"
        :prefix-icon="SearchIcon"
        clearable
      />
    </div>

    <div>
      <div
        v-if="isPending"
        class="text-gray-400 px-3 h-20 flex items-center justify-center"
      >
        <lf-icon
          name="circle-notch"
          class="animate-spin text-gray-400"
          :size="16"
        />
        <span class="text-tiny ml-1 text-gray-400">
          Loading project groups...
        </span>
      </div>
      <div
        v-else-if="projectGroupsList.length"
        class="flex flex-col gap-1 overflow-auto p-2"
      >
        <div
          v-for="projectGroup of projectGroupsList"
          :key="projectGroup.id"
          class="py-1.5 px-2 hover:bg-gray-50 rounded cursor-pointer"
          :class="{
            'bg-primary-50': projectGroup.id === selectedProjectGroup?.id,
          }"
          @click="onOptionClick(projectGroup)"
        >
          <div class="flex gap-0.5 items-start truncate">
            <div class="block truncate mr-2">
              <div class="text-small leading-5 text-gray-900 truncate">
                {{ projectGroup.name }}
              </div>
              <div class="text-tiny text-gray-400">
                {{ pluralize("project", projectGroup.projectCount ?? projectGroup.projects?.length ?? 0, true) }}
              </div>
            </div>
          </div>
        </div>
        <div
          v-if="isFetchingNextPage"
          class="text-gray-400 px-3 h-20 flex items-center justify-center"
        >
          <lf-icon
            name="circle-notch"
            class="animate-spin text-gray-400"
            :size="16"
          />
          <span class="text-tiny ml-1 text-gray-400">
            Loading project groups...
          </span>
        </div>
      </div>
      <div
        v-else
        class="text-gray-400 px-3 h-20 flex items-center justify-center"
      >
        <span class="text-tiny text-gray-400"> No project groups found </span>
      </div>
    </div>
  </el-popover>
</template>

<script setup lang="ts">
import {
  h, ref, onMounted, computed, nextTick, watch,
  onBeforeUnmount,
} from 'vue';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import pluralize from 'pluralize';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import {
  EventType,
  FeatureEventKey,
} from '@/shared/modules/monitoring/types/event';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { QueryFunction, useInfiniteQuery } from '@tanstack/vue-query';
import { Pagination } from '@/shared/types/Pagination';
import { TanstackKey } from '@/shared/types/tanstack';
import { useDebounce } from '@vueuse/core';

import { ToastStore } from '@/shared/message/notification';
import { segmentService } from '../../segments/segments.service';
import { ProjectGroup } from '../../segments/types/Segments';

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

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);
const { updateSelectedProjectGroup } = lsSegmentsStore;

const searchQuery = ref('');
const isPopoverVisible = ref(false);
const searchValue = useDebounce(searchQuery, 300);
const isSearchVisible = computed(() => projectGroupsList.value.length > 5 || searchQuery.value.length > 0);

let scrollContainer: HTMLElement | null = null;

const { trackEvent } = useProductTracking();

const model = computed({
  get() {
    return selectedProjectGroup.value?.name;
  },
  set(id) {
    updateSelectedProjectGroup(id);
  },
});

const queryKey = computed(() => [
  TanstackKey.ADMIN_PROJECT_GROUPS,
  searchValue.value,
]);

const projectGroupsQueryFn = segmentService.queryProjectGroups(() => ({
  limit: 20,
  offset: 0,
  filter: {
    name: searchQuery.value,
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

const projectGroupsList = computed((): ProjectGroup[] => {
  if (isSuccess.value && data.value) {
    return data.value.pages.reduce(
      (acc, page) => acc.concat(page.rows),
      [] as ProjectGroup[],
    );
  }
  return [];
});

// Infinite scroll handler
function onScroll(e: Event) {
  if (!scrollContainer) return;
  const threshold = 40;

  const target = e.target as HTMLElement;
  if (
    !isFetchingNextPage.value
    && hasNextPage.value
    && target.scrollHeight - target.scrollTop - target.clientHeight < threshold
  ) {
    fetchNextPage();
  }
}

watch(error, (err) => {
  if (err) {
    ToastStore.error('Something went wrong while fetching project groups');
  }
});

onMounted(() => {
  nextTick(() => {
    scrollContainer = document.querySelector(
      '.project-groups-select-popper',
    );
    if (scrollContainer) {
      scrollContainer.addEventListener('scroll', onScroll);
    }
  });
});

const onOptionClick = (projectGroup: ProjectGroup) => {
  trackEvent({
    key: FeatureEventKey.SELECT_PROJECT_GROUP,
    type: EventType.FEATURE,
    properties: {
      projectGroupId: projectGroup.id,
      projectName: projectGroup.name,
    },
  });

  isPopoverVisible.value = false;
  updateSelectedProjectGroup(projectGroup);
};

onBeforeUnmount(() => {
  if (scrollContainer) {
    scrollContainer.removeEventListener('scroll', onScroll);
  }
});
</script>

<script lang="ts">
export default {
  name: 'AppLfSubProjectsGroupSelection',
};
</script>

<style lang="scss">
.project-groups-select-popper.el-popper {
  max-width: 236px;
  max-height: 480px;
  overflow: auto;
  @apply p-0;
}

.project-groups-select-input {
  @apply cursor-pointer relative w-full;
  height: 32px !important;

  .el-input__wrapper {
    @apply rounded;
    @apply h-8 px-3;
  }

  .el-input__inner {
    @apply cursor-pointer text-xs truncate pr-6;
  }
}
</style>
