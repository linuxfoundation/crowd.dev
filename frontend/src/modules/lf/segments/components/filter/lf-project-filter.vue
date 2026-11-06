<template>
  <div class="border-b border-gray-100 px-2 pt-2 pb-1 w-full sticky top-0 bg-white z-10">
    <el-input
      id="filterSearch"
      ref="searchQueryInput"
      v-model="searchQuery"
      placeholder="Search..."
      class="lf-filter-input filter-dropdown-search"
      :prefix-icon="SearchIcon"
      data-qa="filter-list-search"
      @input="(value: string) => emit('onSearchChange', value)"
    />
  </div>
  <div class="p-2">
    <app-lf-radio-cascader
      v-model:options="modelOptions"
      empty-text="No projects found"
      :loading="loading"
      @on-change="(value) => emit('onChange', value)"
    />
  </div>
</template>

<script setup lang="ts">
import {
  ref, h, computed,
} from 'vue';
import type { InputInstance } from 'element-plus';
import AppLfRadioCascader from '@/modules/lf/segments/components/filter/lf-radio-cascader.vue';
import type { ProjectsOption } from '@/modules/lf/segments/types/Filters';

defineOptions({ name: 'AppLfProjectFilter' });

const props = withDefaults(defineProps<{
  options?: ProjectsOption[];
  loading?: boolean;
}>(), {
  options: () => [],
  loading: false,
});

const emit = defineEmits<{(e: 'update:options', value: ProjectsOption[]): void;
  (e: 'onChange', value: ProjectsOption[]): void;
  (e: 'onSearchChange', value: string): void;
}>();

const SearchIcon = h(
  'i', // type
  { class: 'fa-light fa-magnifying-glass c-icon' }, // props
);

const modelOptions = computed({
  get() {
    return props.options;
  },
  set(v: ProjectsOption[]) {
    emit('update:options', v);
  },
});

const searchQuery = ref('');
const searchQueryInput = ref<InputInstance | null>(null);
</script>

<style lang="scss">
.lf-filter-input {
  @apply h-10;

  .el-input__wrapper {
    @apply px-4;
  }
}
</style>
