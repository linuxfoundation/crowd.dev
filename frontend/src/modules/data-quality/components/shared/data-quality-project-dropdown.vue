<template>
  <lf-dropdown>
    <template #trigger>
      <lf-button type="secondary">
        <lf-icon name="layer-group" />
        <div class="truncate" style="max-width: 28ch;">
          {{ getProjectName }}
        </div>
      </lf-button>
    </template>
    <div class="-mx-2 -mt-2 mb-2 min-w-64" @click.stop>
      <lf-input
        v-model="search"
        placeholder="Search..."
        class=" !border-0 !rounded-none shadow-none"
      >
        <template #prefix>
          <lf-icon name="magnifying-glass" />
        </template>
      </lf-input>
    </div>

    <div class="w-full max-h-60 overflow-auto gap-1 flex flex-col">
      <lf-dropdown-item
        v-for="group of options"
        :key="group.id"
        :selected="project === group.id"
        @click="changeOption(group.id)"
      >
        {{ group.name }}
      </lf-dropdown-item>
    </div>
  </lf-dropdown>
</template>

<script lang="ts" setup>
import LfButton from '@/ui-kit/button/Button.vue';
import { computed, ref } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import LfInput from '@/ui-kit/input/Input.vue';

defineOptions({
  name: 'LfDataQualityProjectDropdown',
});

const props = defineProps<{
  modelValue: string;
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: string): void}>();

const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());

const search = ref('');

const project = computed({
  get: () => props.modelValue,
  set: (value: string) => emit('update:modelValue', value),
});

const options = computed(
  () => ([
    {
      id: selectedProjectGroup.value?.id,
      name: 'All projects',
    },
    ...(selectedProjectGroup.value?.projects || []).filter((group) => group.name.toLowerCase().includes(search.value.toLowerCase())),
  ]),
);

const changeOption = (group?: string) => {
  // shortcut: cast hides an undefined id; the route requires a project group. revisit: if that guard goes
  project.value = group as string;
  search.value = '';
};

const getProjectName = computed(() => {
  if (project.value === selectedProjectGroup.value?.id) {
    return 'All projects';
  }
  return selectedProjectGroup.value?.projects?.find((group) => group.id === project.value)?.name;
});
</script>
