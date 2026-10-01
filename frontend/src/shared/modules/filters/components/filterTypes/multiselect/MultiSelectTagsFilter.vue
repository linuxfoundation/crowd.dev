<template>
  <div v-if="form" class="px-4 pt-3">
    <el-select
      v-model="form"
      multiple
      filterable
      remote
      :reserve-keyword="false"
      placeholder="Select options"
      :remote-method="searchOptions"
      :teleported="false"
      class="filter-multiselect"
      popper-class="filter-multiselect-popper"
      no-data-text="No results"
    >
      <template v-for="(group, gi) of filteredOptions" :key="gi">
        <div
          v-if="group.label && group.options.length > 0"
          class="text-2xs text-gray-400 font-semibold tracking-wide leading-6 uppercase px-3 my-1"
        >
          {{ group.label }}
        </div>
        <el-option
          v-for="option of group.options"
          :key="option.value"
          :label="option.label"
          :value="option.value"
          data-qa="filter-select-option"
          :data-qa-value="option.value"
        >
          <lf-checkbox class="filter-checkbox h-4" :model-value="form.includes(option.value)">
            {{ option.label }}
          </lf-checkbox>
        </el-option>
      </template>
    </el-select>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue';
import type {
  MultiSelectFilterOptions,
  MultiSelectFilterConfig, MultiSelectFilterOptionGroup,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import LfCheckbox from '@/ui-kit/checkbox/Checkbox.vue';

const props = defineProps<{
  modelValue: string[],
  config: MultiSelectFilterConfig
} & MultiSelectFilterOptions>();

const emit = defineEmits<{(e: 'update:modelValue', value: string[]): void}>();

const form = computed<string[]>({
  get: () => props.modelValue,
  set: (value: string[]) => emit('update:modelValue', value),
});

const filteredOptions = ref<MultiSelectFilterOptionGroup[]>(props.options);

const searchOptions = (query: string) => {
  filteredOptions.value = props.options.map((g) => ({
    ...g,
    options: g.options.filter((o) => o.label.toLowerCase().includes(query.toLowerCase())),
  }));
};

onMounted(() => {
  searchOptions('');
});
</script>

<script lang="ts">
export default {
  name: 'LfMultiSelectTagsFilter',
};
</script>

<style lang="scss">
.filter-multiselect {
  @apply w-full relative;

  .el-select__tags{
    @apply top-1.5 transform-none;
  }

  .el-select-dropdown__item {
    @apply px-3 #{!important};

    &.selected{
      @apply bg-primary-25 font-normal px-3 #{!important};
    }

    &:after{
      @apply hidden;
    }
  }

  .el-input__wrapper,
  .el-input__wrapper.is-focus,
  .el-input__wrapper:hover {
    @apply bg-gray-50 shadow-none rounded-md border border-gray-50 transition #{!important};

    input {
      &,
      &:hover,
      &:focus {
        border: none !important;
        @apply bg-gray-50 shadow-none outline-none h-full min-h-8;
      }
    }
  }

  .el-tag{
    @apply bg-white #{!important};
  }
}
.filter-multiselect-popper {
  @apply relative inset-0 block shadow-none h-auto opacity-100 transform-none #{!important};

  .el-select-dropdown{
    @apply -mx-4 p-0 mt-3 border-t border-gray-100;

    .el-scrollbar__view{
      @apply py-3 px-3;
    }
  }
}
</style>
