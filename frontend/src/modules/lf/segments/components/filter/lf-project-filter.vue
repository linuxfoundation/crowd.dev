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
      @input="(value) => emit('onSearchChange', value)"
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

<script setup>
import {
  ref, h, computed,
} from 'vue';
import AppLfRadioCascader from '@/modules/lf/segments/components/filter/lf-radio-cascader.vue';

const props = defineProps({
  options: {
    type: Array,
    default: () => [],
  },
  loading: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits(['update:options', 'onChange', 'onSearchChange']);

const SearchIcon = h(
  'i', // type
  { class: 'fa-light fa-magnifying-glass c-icon' }, // props
);

const modelOptions = computed({
  get() {
    return props.options;
  },
  set(v) {
    emit('update:options', v);
  },
});

const searchQuery = ref('');
const searchQueryInput = ref(null);
</script>

<script>
export default {
  name: 'AppLfProjectFilter',
};
</script>

<style lang="scss">
.lf-filter-input {
  @apply h-10;

  .el-input__wrapper {
    @apply px-4;
  }
}
</style>
