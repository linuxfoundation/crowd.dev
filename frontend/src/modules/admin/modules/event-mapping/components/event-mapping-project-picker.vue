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
    <div class="flex w-56 flex-col gap-1 rounded-xl border border-gray-100 bg-white p-2 shadow-md">
      <lf-search
        v-model="search"
        lazy
        placeholder="Search projects…"
        class="!h-9"
      />
      <div class="max-h-65 overflow-y-auto" @scroll="onScroll">
        <div v-if="isPending" class="flex justify-center py-4">
          <lf-spinner size="1.5rem" />
        </div>
        <div v-else-if="isError && projects.length === 0" class="flex flex-col items-start gap-2 p-2">
          <p class="text-xs text-gray-500">
            Could not load projects.
          </p>
          <lf-button type="secondary" size="small" @click="refetch()">
            Retry
          </lf-button>
        </div>
        <template v-else>
          <div v-for="group of groups" :key="group.groupName">
            <p class="px-2 pb-1 pt-2 text-2xs font-bold uppercase tracking-wider text-gray-500">
              {{ group.groupName }}
            </p>
            <lf-dropdown-item
              v-for="project of group.projects"
              :key="project.segmentId"
              :selected="project.segmentId === props.currentSegmentId"
              @click="pick(project)"
            >
              {{ project.name }}
            </lf-dropdown-item>
          </div>
          <p v-if="groups.length === 0" class="p-2 text-xs text-gray-500">
            No project matches.
          </p>
          <div v-if="isFetchingNextPage" class="flex justify-center py-2">
            <lf-spinner size="1rem" />
          </div>
        </template>
      </div>
      <lf-dropdown-separator />
      <p class="px-2 py-1 text-2xs text-gray-500">
        Only projects are listed. Project groups can't be picked.
      </p>
    </div>
  </lf-popover>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import LfPopover from '@/ui-kit/popover/Popover.vue';
import LfSearch from '@/ui-kit/search/Search.vue';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfDropdownSeparator from '@/ui-kit/dropdown/DropdownSeparator.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import { ToastStore } from '@/shared/message/notification';
import { groupProjects } from '@/modules/admin/modules/event-mapping/event-mapping-helper';
import { useEventMappingProjectsQuery } from '@/modules/admin/modules/event-mapping/queries/useEventMappingProjectsQuery';
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

const SCROLL_THRESHOLD = 20;

const search = ref<string>('');

const {
  projects,
  isPending,
  isFetchingNextPage,
  fetchNextPage,
  hasNextPage,
  isError,
  error,
  refetch,
  reset,
} = useEventMappingProjectsQuery(search, computed(() => props.open));

const groups = computed(() => groupProjects(projects.value));

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
      reset();
    }
  },
);

watch(error, (err) => {
  if (err && props.open) {
    ToastStore.error('Something went wrong while fetching projects');
  }
});
</script>
