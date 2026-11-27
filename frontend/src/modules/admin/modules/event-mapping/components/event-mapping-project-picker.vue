<template>
  <lf-popover
    :visibility="props.open"
    placement="bottom-start"
    is-dropdown
    @update:visibility="emit('update:open', $event)"
  >
    <template #trigger>
      <slot name="trigger" />
    </template>
    <div class="w-56 overflow-hidden rounded-lg border border-gray-300 bg-white shadow-lg">
      <div class="border-b border-gray-100 p-2">
        <lf-search
          v-model="search"
          lazy
          placeholder="Search projects…"
          class="!h-9"
        />
      </div>
      <div class="max-h-65 overflow-y-auto py-1" @scroll="onScroll">
        <div v-if="isPending" class="flex justify-center py-4">
          <lf-spinner size="1.5rem" />
        </div>
        <template v-else>
          <div v-for="group of groups" :key="group.groupName">
            <p class="px-3 pb-1 pt-2 text-2xs font-bold uppercase tracking-wider text-gray-500">
              {{ group.groupName }}
            </p>
            <lf-dropdown-item
              v-for="project of group.projects"
              :key="project.segmentId"
              :selected="project.segmentId === props.currentSegmentId"
              class="!rounded-none !px-3"
              @click="pick(project)"
            >
              {{ project.name }}
            </lf-dropdown-item>
          </div>
          <p v-if="groups.length === 0" class="p-3 text-xs text-gray-500">
            No project matches.
          </p>
          <div v-if="isFetchingNextPage" class="flex justify-center py-2">
            <lf-spinner size="1rem" />
          </div>
        </template>
      </div>
      <p class="border-t border-gray-100 bg-gray-50 px-3 py-2 text-2xs text-gray-500">
        Only projects are listed. Project groups can't be picked.
      </p>
    </div>
  </lf-popover>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import type { QueryFunction } from '@tanstack/vue-query';
import { useInfiniteQuery } from '@tanstack/vue-query';
import LfPopover from '@/ui-kit/popover/Popover.vue';
import LfSearch from '@/ui-kit/search/Search.vue';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import { INSIGHTS_PROJECTS_SERVICE } from '@/modules/admin/modules/insights-projects/services/insights-projects.service';
import { queryKeys } from '@/shared/queries/keys';
import type { Pagination } from '@/shared/types/Pagination';
import type { Project } from '@/modules/lf/segments/types/Segments';
import { ToastStore } from '@/shared/message/notification';
import { groupProjects } from '@/modules/admin/modules/event-mapping/event-mapping-helper';
import type { EventMappingProject } from '@/modules/admin/modules/event-mapping/types/EventMapping';

defineOptions({
  name: 'LfEventMappingProjectPicker',
});

const props = defineProps<{
  open: boolean;
  currentSegmentId?: string;
}>();

const emit = defineEmits<{(e: 'update:open', value: boolean): void;
  (e: 'pick', value: EventMappingProject): void;
}>();

const PAGE_SIZE = 50;
const SCROLL_THRESHOLD = 20;

const search = ref<string>('');

const queryFn = INSIGHTS_PROJECTS_SERVICE.querySubProjects(() => ({
  limit: PAGE_SIZE,
  offset: 0,
  filter: {
    status: 'active',
    ...(search.value ? { name: search.value } : {}),
  },
})) as QueryFunction<Pagination<Project>, readonly unknown[], unknown>;

const {
  data,
  isPending,
  isFetchingNextPage,
  fetchNextPage,
  hasNextPage,
  error,
} = useInfiniteQuery<Pagination<Project>, Error>({
  queryKey: computed(() => queryKeys.subProjects.activeSearch(search.value)),
  queryFn,
  getNextPageParam: (lastPage) => {
    const nextPage = lastPage.offset + lastPage.limit;
    return nextPage < lastPage.count ? nextPage : undefined;
  },
  initialPageParam: 0,
  enabled: computed(() => props.open),
});

const groups = computed(() => groupProjects(data.value?.pages.flatMap((page) => page.rows) ?? []));

const onScroll = (event: Event) => {
  const target = event.target as HTMLElement;
  if (
    hasNextPage.value
    && !isFetchingNextPage.value
    && target.scrollHeight - target.scrollTop - target.clientHeight < SCROLL_THRESHOLD
  ) {
    fetchNextPage();
  }
};

const pick = (project: EventMappingProject) => {
  emit('pick', project);
  emit('update:open', false);
};

watch(
  () => props.open,
  (open) => {
    if (!open) {
      search.value = '';
    }
  },
);

watch(error, (err) => {
  if (err) {
    ToastStore.error('Something went wrong while fetching projects');
  }
});
</script>
