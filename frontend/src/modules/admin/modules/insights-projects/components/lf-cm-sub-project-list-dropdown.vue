<template>
  <app-form-item
    class="mb-6"
    :validation="$v.projectId"
    label="Project"
    :required="true"
    :error-messages="{
      required: 'Project is required',
    }"
  >
    <span class="text-2xs text-gray-500">
      Select an active project from Community Data Platform
    </span>
    <div class="relative">
      <div v-if="form.projectId" class="absolute top-2 left-2 z-10">
        <lf-avatar
          :src="form.project.url"
          :name="form.project.name"
          :size="24"
          class="!rounded-md border border-gray-200"
        />
      </div>
      <el-input
        ref="inputRef"
        v-model="inputValue"
        class="subprojects-select-input"
        :class="{
          'subprojects-select-active': form.projectId,
        }"
        placeholder="Select option"
        readonly
        :suffix-icon="isPopoverVisible ? ArrowUpIcon : ArrowDownIcon"
        @blur="$v.projectId.$touch"
        @click="isPopoverVisible = true"
      />
    </div>
  </app-form-item>

  <el-popover
    v-model:visible="isPopoverVisible"
    :virtual-ref="inputRef"
    placement="bottom"
    trigger="manual"
    virtual-triggering
    popper-class="subprojects-select-popper"
    :teleported="false"
  >
    <div class="mb-2 border-b border-gray-100 px-2 pt-2 pb-1 w-full">
      <el-input
        id="filterSearch"
        v-model="searchQuery"
        placeholder="Search..."
        class="filter-dropdown-search"
        :prefix-icon="SearchIcon"
      />
    </div>

    <div class="p-2">
      <div v-if="projectsList.length" class="flex flex-col gap-4">
        <div
          v-for="project in projectsList"
          :key="project.id"
          class="h-10 flex items-center px-3 text-xs text-gray-900 hover:bg-gray-50 rounded cursor-pointer"
          :class="{
            'bg-primary-50': project.id === form.projectId,
          }"
          @click="onOptionClick(project)"
        >
          <lf-avatar
            :src="project.url"
            :name="project.name"
            :size="24"
            class="!rounded-md border border-gray-200"
          />
          <span class="ml-2 text-gray-900 text-sm">{{ project.name }}</span>
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
          <span class="text-tiny ml-1 text-gray-400">Loading projects...</span>
        </div>
      </div>
      <div
        v-else-if="isPending"
        class="text-gray-400 p-3 py-6 h-10 flex items-center justify-center"
      >
        <lf-spinner />
      </div>
      <div v-else class="text-gray-400 px-3 h-10 flex items-center">
        No projects found
      </div>
    </div>
  </el-popover>
</template>

<script setup lang="ts">
import useVuelidate from '@vuelidate/core';
import { required } from '@vuelidate/validators';
import {
  reactive, h, ref, onMounted, computed, nextTick, onBeforeUnmount, watch,
} from 'vue';
import AppFormItem from '@/shared/form/form-item.vue';
import { INSIGHTS_PROJECTS_SERVICE } from '@/modules/admin/modules/insights-projects/services/insights-projects.service';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import { useDebounce } from '@vueuse/core';
import { TanstackKey } from '@/shared/types/tanstack';
import type { QueryFunction } from '@tanstack/vue-query';
import { useInfiniteQuery } from '@tanstack/vue-query';
import type { Pagination } from '@/shared/types/Pagination';
import type { Project } from '@/modules/lf/segments/types/Segments';

import { ToastStore } from '@/shared/message/notification';

const props = defineProps<{
  selectedProjectId: string;
}>();

const emit = defineEmits<{(e: 'onChange', value: any): void }>();

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

const inputRef = ref(null);
const searchQuery = ref('');
const searchValue = useDebounce(searchQuery, 300);
let scrollContainer: HTMLElement | null = null;
const isPopoverVisible = ref(false);

const inputValue = ref('');
const form = reactive<{
  projectId: string;
  project: {
    id: string | undefined;
    name: string;
    url: string;
  };
}>({
  projectId: '',
  project: {
    id: undefined,
    name: '',
    url: '',
  },
});

const rules = {
  projectId: {
    required,
  },
};

const $v = useVuelidate(rules, form);

const queryKey = computed(() => [
  TanstackKey.ADMIN_SUB_PROJECTS,
  searchValue.value,
]);

const queryFn = INSIGHTS_PROJECTS_SERVICE.querySubProjects(() => ({
  limit: 20,
  offset: 0,
  filter: searchValue.value
    ? {
      name: searchValue.value,
    }
    : {},
})) as QueryFunction<Pagination<Project>, readonly unknown[], unknown>;

const {
  data,
  isPending,
  isFetchingNextPage,
  fetchNextPage,
  hasNextPage,
  isSuccess,
  error,
} = useInfiniteQuery<Pagination<Project>, Error>({
  queryKey,
  queryFn,
  getNextPageParam: (lastPage) => {
    const nextPage = lastPage.offset + lastPage.limit;
    const totalRows = lastPage.count;
    return nextPage < totalRows ? nextPage : undefined;
  },
  initialPageParam: 0,
});

const projectsList = computed((): Project[] => {
  if (isSuccess.value && data.value) {
    return data.value.pages.reduce(
      (acc, page) => acc.concat(page.rows),
      [] as Project[],
    );
  }
  return [];
});

watch(error, (err) => {
  if (err) {
    ToastStore.error('Something went wrong while fetching projects');
  }
});

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

const onOptionClick = (project: Project) => {
  isPopoverVisible.value = false;

  form.projectId = project.id;
  form.project = project;
  inputValue.value = project.name;
  emit('onChange', {
    project,
    isSubmitEnabled: !$v.value.$invalid,
  });
};

onMounted(() => {
  if (props.selectedProjectId) {
    form.projectId = props.selectedProjectId;
    inputValue.value = props.selectedProjectId;
    return;
  }
  nextTick(() => {
    scrollContainer = document.querySelector(
      '.subprojects-select-popper',
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
  name: 'LfCmSubProjectListDropdown',
};
</script>

<style lang="scss">
.subprojects-select-popper.el-popper {
  width: 90% !important;
  max-height: 480px;
  overflow: auto;
  @apply p-0;
}

.subprojects-select-input {
  @apply cursor-pointer relative w-full;

  .el-input__inner {
    @apply cursor-pointer;
  }
}

.subprojects-select-active .el-input__inner {
  @apply pl-6;
}
</style>
