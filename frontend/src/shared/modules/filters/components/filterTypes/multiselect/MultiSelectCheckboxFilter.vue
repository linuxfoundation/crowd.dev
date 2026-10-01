<template>
  <div class="max-h-60 overflow-auto py-3 px-2">
    <template v-for="(group, gi) of props.options" :key="gi">
      <div
        v-if="group.label && group.options.length > 0"
        class="text-2xs text-gray-400 font-semibold tracking-wide leading-6 uppercase px-3 my-1"
      >
        {{ group.label }}
      </div>
      <lf-filter-multi-select-option v-for="(option, oi) of group.options" :key="oi" v-model="form" :value="option.value">
        {{ option.label }}
      </lf-filter-multi-select-option>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import type {
  MultiSelectFilterOptions,
  MultiSelectFilterConfig,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import LfFilterMultiSelectOption
  from '@/shared/modules/filters/components/partials/multiselect/FilterMultiSelectOption.vue';

const props = defineProps<{
  modelValue: string[],
  config: MultiSelectFilterConfig
} & MultiSelectFilterOptions>();

const emit = defineEmits<{(e: 'update:modelValue', value: string[]): void}>();

const form = computed({
  get: () => props.modelValue,
  set: (value: string[]) => emit('update:modelValue', value),
});
</script>

<script lang="ts">
export default {
  name: 'LfMultiSelectCheckboxFilter',
};
</script>
