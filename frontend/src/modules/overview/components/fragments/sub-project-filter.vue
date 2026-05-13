<template>
  <lfx-dropdown-select
    v-model="selectedProjectId"
    width="255px"
    :match-width="false"
    dropdown-class="max-h-80"
    placement="bottom-end"
    :disabled="isPending"
  >
    <template #trigger>
      <lfx-dropdown-selector
        size="medium"
        type="filled"
        class="flex items-center justify-center w-full !rounded-l-none"
      >
        <div class="flex items-center gap-2">
          <lf-icon
            name="layer-group"
            :size="16"
          />
          <span class="text-sm text-neutral-900 truncate" :class="{ 'font-semibold': selectedSubProject }">
            {{ trimDisplay(selectedSubProject?.name || '') || 'All sub-projects' }}
          </span>
        </div>
      </lfx-dropdown-selector>
    </template>

    <template #default>
      <div class="sticky -top-1 z-10 bg-white w-full -mt-1 pt-1 flex flex-col gap-1">
        <!-- All projects option -->
        <lfx-dropdown-item
          value="all"
          label="All-subprojects"
          :selected="!selectedSubProject"
          :class="{
            '!bg-blue-50': !selectedSubProject,
          }"
          @click="selectedSubProjectId = ''"
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
        v-if="!subProjectsList.length && searchQuery"
        class="py-4 px-3 text-sm text-neutral-500 text-center"
      >
        No projects found
      </div>

      <template v-else>
        <lfx-dropdown-item
          v-for="subProject in subProjectsList"
          :key="subProject.id"
          :value="subProject.id"
          :label="subProject.name"
          :selected="selectedSubProject?.id === subProject.id"
          :class="{
            '!bg-blue-50': selectedSubProject?.id === subProject.id,
          }"
          @click="selectedSubProjectId = subProject.id"
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

import { SubProject } from '@/modules/lf/segments/types/Segments';
import LfxDropdownSelect from '@/ui-kit/lfx/dropdown/dropdown-select.vue';
import LfxDropdownSelector from '@/ui-kit/lfx/dropdown/dropdown-selector.vue';
import LfxDropdownItem from '@/ui-kit/lfx/dropdown/dropdown-item.vue';
import LfxDropdownSeparator from '@/ui-kit/lfx/dropdown/dropdown-separator.vue';
import LfxDropdownSearch from '@/ui-kit/lfx/dropdown/dropdown-search.vue';
import { useOverviewStore } from '../../store/overview.store';
import { OVERVIEW_API_SERVICE } from '../../services/overview.api.service';

const overviewStore = useOverviewStore();
const { selectedProjectId, selectedSubProjectId, selectedSubProject } = storeToRefs(overviewStore);

const searchQuery = ref('');
const searchValue = useDebounce(searchQuery, 300);

// const props = defineProps<{
//   subProjects: Project[];
// }>();

const params = computed(() => selectedProjectId.value || '');

const { data, isPending } = OVERVIEW_API_SERVICE.fetchProjectById(params);

const trimDisplay = (name: string) => (name.length > 20 ? `${name.slice(0, 20)}...` : name);

const subProjectsList = computed<SubProject[]>(() => (data.value?.subprojects || []).filter((subProject) => {
  if (!searchValue.value) return true;
  return subProject.name.toLowerCase().includes(searchValue.value.toLowerCase());
}));

watch(selectedSubProjectId, (newVal) => {
  if (newVal && newVal !== '') {
    selectedSubProject.value = subProjectsList.value?.find((p) => p.id === newVal) || null;
  } else {
    selectedSubProject.value = null;
  }
}, { immediate: true });
</script>

<script lang="ts">
export default {
  name: 'AppLfSubProjectFilter',
};
</script>
