<template>
  <lfx-dropdown-select
    v-model="selectedProjectId"
    width="255px"
    :match-width="false"
    dropdown-class="max-h-80"
    placement="bottom-end"
  >
    <template #trigger>
      <lfx-dropdown-selector
        size="medium"
        type="filled"
        class="flex items-center justify-center w-full !rounded-l-none"
        :class="{
          '!rounded-r-none': selectedProject,
        }"
      >
        <div class="flex items-center gap-2">
          <lf-icon
            name="folders"
            :size="16"
          />
          <span class="text-sm text-neutral-900 truncate" :class="{ 'font-semibold': selectedProject }">
            {{ trimDisplay(selectedProject?.name || '') || 'All projects' }}
          </span>
        </div>
      </lfx-dropdown-selector>
    </template>

    <template #default>
      <div class="sticky -top-1 z-10 bg-white w-full -mt-1 pt-1 flex flex-col gap-1">
        <!-- All projects option -->
        <lfx-dropdown-item
          value="all"
          label="All projects"
          :selected="!selectedProject"
          :class="{
            '!bg-blue-50': !selectedProject,
          }"
          @click="selectedProjectId = ''"
        />

        <lfx-dropdown-separator />

        <!-- Search input -->
        <lfx-dropdown-search
          v-model="searchQuery"
          placeholder="Search projects..."
          lazy
          class=""
        />

        <lfx-dropdown-separator />
      </div>

      <!-- Projects list -->
      <div
        v-if="!projectsList.length && searchQuery"
        class="py-4 px-3 text-sm text-neutral-500 text-center"
      >
        No projects found
      </div>

      <template v-else>
        <lfx-dropdown-item
          v-for="project in projectsList"
          :key="project.id"
          :value="project.id"
          :label="project.name"
          :selected="selectedProject?.id === project.id"
          :class="{
            '!bg-blue-50': selectedProject?.id === project.id,
          }"
          @click="selectProject(project.id)"
        />
      </template>
    </template>
  </lfx-dropdown-select>
</template>

<script setup lang="ts">
import {
  ref,
  computed,
  watch,
} from 'vue';
import { storeToRefs } from 'pinia';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { useDebounce } from '@vueuse/core';

import { Project } from '@/modules/lf/segments/types/Segments';
import LfxDropdownSelect from '@/ui-kit/lfx/dropdown/dropdown-select.vue';
import LfxDropdownSelector from '@/ui-kit/lfx/dropdown/dropdown-selector.vue';
import LfxDropdownItem from '@/ui-kit/lfx/dropdown/dropdown-item.vue';
import LfxDropdownSeparator from '@/ui-kit/lfx/dropdown/dropdown-separator.vue';
import LfxDropdownSearch from '@/ui-kit/lfx/dropdown/dropdown-search.vue';
import { useOverviewStore } from '../../store/overview.store';

const overviewStore = useOverviewStore();
const { selectedProject, selectedProjectId } = storeToRefs(overviewStore);

const searchQuery = ref('');
const searchValue = useDebounce(searchQuery, 300);

const props = defineProps<{
  projects: Project[];
}>();

const trimDisplay = (name: string) => (name.length > 20 ? `${name.slice(0, 20)}...` : name);

const selectProject = (projectId: string) => {
  selectedProjectId.value = projectId;
};

const projectsList = computed(() => {
  const filtered = props.projects.filter((project) => {
    if (!searchValue.value) return true;
    return project.name.toLowerCase().includes(searchValue.value.toLowerCase());
  });

  return filtered.map((project) => ({
    id: project.id,
    name: project.name,
  }));
});

watch(selectedProjectId, (newVal) => {
  if (newVal && newVal !== '') {
    selectedProject.value = props.projects.find((p) => p.id === newVal) || null;
  } else {
    selectedProject.value = null;
  }
}, { immediate: true });
</script>

<script lang="ts">
export default {
  name: 'AppLfProjectFilter',
};
</script>
