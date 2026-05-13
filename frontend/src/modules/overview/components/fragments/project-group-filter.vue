<template>
  <lfx-dropdown-select
    v-model="selectedProjectGroupId"
    width="255px"
    :match-width="false"
    dropdown-class="max-h-80"
    placement="bottom-end"
  >
    <template #trigger>
      <lfx-dropdown-selector
        size="medium"
        type="filled"
        class="flex items-center justify-center w-full"
        :class="{
          '!rounded-r-none': selectedProjectGroup,
        }"
      >
        <div class="flex items-center gap-2">
          <lf-icon
            name="folder-tree"
            :size="16"
          />
          <span class="text-sm text-neutral-900 truncate" :class="{ 'font-semibold': selectedProjectGroup }">
            {{ trimDisplay(selectedProjectGroup?.name || '') || 'All project groups' }}
          </span>
        </div>
      </lfx-dropdown-selector>
    </template>

    <template #default>
      <div class="sticky -top-1 z-10 bg-white w-full -mt-1 pt-1 flex flex-col gap-1">
        <!-- All project groups option -->
        <lfx-dropdown-item
          value="all"
          label="All project groups"
          :selected="!selectedProjectGroup"
          :class="{
            '!bg-blue-50': !selectedProjectGroup,
          }"
          @click="selectedProjectGroupId = ''"
        />

        <lfx-dropdown-separator />

        <!-- Search input -->
        <lfx-dropdown-search
          v-model="searchQuery"
          placeholder="Search project groups..."
          lazy
          class=""
        />

        <lfx-dropdown-separator />
      </div>

      <!-- Project groups list -->
      <div
        v-if="isPending"
        class="py-8 flex justify-center"
      >
        <lf-spinner />
      </div>

      <div
        v-else-if="!projectGroupsList.length && searchQuery"
        class="py-4 px-3 text-sm text-neutral-500 text-center"
      >
        No project groups found
      </div>

      <template v-else>
        <lfx-dropdown-item
          v-for="projectGroup in projectGroupsList"
          :key="projectGroup.id"
          :value="projectGroup.id"
          :label="projectGroup.name"
          :selected="selectedProjectGroup?.id === projectGroup.id"
          :class="{
            '!bg-blue-50': selectedProjectGroup?.id === projectGroup.id,
          }"
          @click="selectedProjectGroupId = projectGroup.id"
        />
      </template>

      <div
        v-if="isFetchingNextPage"
        class="py-8 flex justify-center"
      >
        <lf-spinner />
      </div>

      <app-lf-load-more
        v-if="!isFetchingNextPage && hasNextPage"
        :is-fetching-next-page="isFetchingNextPage"
        text="Loading more project groups..."
        @load-more="fetchNextPage"
      />
    </template>
  </lfx-dropdown-select>
</template>

<script setup lang="ts">
import {
  ref, computed, watch,
} from 'vue';
import { storeToRefs } from 'pinia';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { QueryFunction, useInfiniteQuery } from '@tanstack/vue-query';
import { Pagination } from '@/shared/types/Pagination';
import { TanstackKey } from '@/shared/types/tanstack';
import { useDebounce } from '@vueuse/core';

import { ToastStore } from '@/shared/message/notification';
import { segmentService } from '@/modules/lf/segments/segments.service';
import { ProjectGroup } from '@/modules/lf/segments/types/Segments';
import LfxDropdownSelect from '@/ui-kit/lfx/dropdown/dropdown-select.vue';
import LfxDropdownSelector from '@/ui-kit/lfx/dropdown/dropdown-selector.vue';
import LfxDropdownItem from '@/ui-kit/lfx/dropdown/dropdown-item.vue';
import LfxDropdownSeparator from '@/ui-kit/lfx/dropdown/dropdown-separator.vue';
import LfxDropdownSearch from '@/ui-kit/lfx/dropdown/dropdown-search.vue';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import { useOverviewStore } from '../../store/overview.store';
import AppLfLoadMore from './load-more.vue';

const overviewStore = useOverviewStore();
const {
  selectedProjectGroup,
  selectedProjectGroupId,
  selectedProjectId,
  selectedSubProjectId,
  selectedProject,
  selectedSubProject,
} = storeToRefs(overviewStore);

const searchQuery = ref('');
const searchValue = useDebounce(searchQuery, 300);

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

const trimDisplay = (name: string) => (name.length > 20 ? `${name.slice(0, 20)}...` : name);

watch(error, (err) => {
  if (err) {
    ToastStore.error('Something went wrong while fetching project groups');
  }
});

watch(selectedProjectGroupId, (newVal) => {
  if (newVal && newVal !== '') {
    selectedProjectGroup.value = projectGroupsList.value.find((pg) => pg.id === newVal) || null;
  } else {
    selectedProjectGroup.value = null;
    selectedProjectId.value = '';
    selectedSubProjectId.value = '';
    selectedProject.value = null;
    selectedSubProject.value = null;
  }
}, { immediate: true });
</script>

<script lang="ts">
export default {
  name: 'AppLfProjectGroupFilter',
};
</script>
