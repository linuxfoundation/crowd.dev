<template>
  <div v-if="form">
    <div class="p-4 pb-5">
      <lf-filter-inline-select
        v-if="!props.config.options.fixedOperator"
        v-model="form.operator"
        :prefix="`${props.config.label}:`"
        class="mb-3"
        :options="stringFilterOperators"
      />

      <lf-filter-input
        v-model="form.value"
        placeholder="Enter a value"
        data-qa="filter-input"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted } from 'vue';
import useVuelidate from '@vuelidate/core';
import { required } from '@vuelidate/validators';
import type {
  StringFilterConfig,
  StringFilterOptions,
  StringFilterValue,
} from '@/shared/modules/filters/types/filterTypes/StringFilterConfig';
import LfFilterInlineSelect from '@/shared/modules/filters/components/partials/FilterInlineSelect.vue';
import LfFilterInput from '@/shared/modules/filters/components/partials/string/FilterInput.vue';
import { stringFilterOperators, FilterStringOperator } from '@/shared/modules/filters/config/constants/string.constants';

const props = defineProps<{
  modelValue: StringFilterValue,
  config: StringFilterConfig
} & StringFilterOptions>();

const emit = defineEmits<{(e: 'update:modelValue', value: StringFilterValue): void}>();

const form = computed<StringFilterValue>({
  get: () => props.modelValue,
  set: (value: StringFilterValue) => emit('update:modelValue', value),
});

const defaultForm: StringFilterValue = {
  value: '',
  operator: props.config?.options?.fixedOperator || FilterStringOperator.LIKE,
};

const rules: any = {
  value: {
    required,
  },
  operator: {
    required,
  },
};

useVuelidate(rules, form);

onMounted(() => {
  form.value = {
    ...defaultForm,
    ...form.value,
  };
});
</script>

<script lang="ts">
export default {
  name: 'LfStringFilter',
};
</script>
