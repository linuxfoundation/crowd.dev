<template>
  <div v-if="form">
    <lf-filter-include-switch v-if="!props.hideIncludeSwitch" v-model="form.include" />
    <div v-if="allOptions.length <= 7">
      <lf-multi-select-checkbox-filter
        v-model="form.value"
        :config="props.config"
        :options="props.options"
      />
    </div>
    <div v-else>
      <lf-multi-select-tags-filter
        v-model="form.value"
        :config="props.config"
        :options="props.options"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted } from 'vue';
import type {
  MultiSelectFilterValue,
  MultiSelectFilterOptions,
  MultiSelectFilterConfig,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import { required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import LfFilterIncludeSwitch from '@/shared/modules/filters/components/partials/FilterIncludeSwitch.vue';
import LfMultiSelectCheckboxFilter
  from '@/shared/modules/filters/components/filterTypes/multiselect/MultiSelectCheckboxFilter.vue';
import LfMultiSelectTagsFilter
  from '@/shared/modules/filters/components/filterTypes/multiselect/MultiSelectTagsFilter.vue';

const props = defineProps<{
  modelValue: MultiSelectFilterValue,
  config: MultiSelectFilterConfig
} & MultiSelectFilterOptions>();

const emit = defineEmits<{(e: 'update:modelValue', value: MultiSelectFilterValue): void, (e: 'update:data', value: any): void}>();

const allOptions = computed(() => props.options.map((g) => g.options).flat());

const form = computed({
  get: () => props.modelValue,
  set: (value: MultiSelectFilterValue) => emit('update:modelValue', value),
});

const defaultForm: MultiSelectFilterValue = {
  value: [],
  include: true,
};

const rules: any = {
  value: {
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
  name: 'LfMultiSelectFilter',
};
</script>
