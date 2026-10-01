<template>
  <div v-if="form">
    <lf-filter-include-switch v-if="!props.hideIncludeSwitch" v-model="form.include" />
    <div v-if="allOptions.length > 7" class="border-b border-gray-100 px-2 py-1">
      <el-input
        ref="queryInput"
        v-model="search"
        placeholder="Search..."
        class="filter-dropdown-search"
      >
        <template #prefix>
          <lf-icon name="search" :size="16" />
        </template>
        <template #suffix>
          <lf-icon v-if="search.length > 0" name="xmark" :size="16" class="cursor-pointer" @click="search = ''" />
        </template>
      </el-input>
    </div>
    <div class="max-h-58 overflow-auto pb-3 px-2 pt-1">
      <template v-for="(group, gi) of filteredOptions" :key="gi">
        <div
          v-if="group.label && group.options.length > 0"
          class="text-2xs text-gray-400 font-semibold tracking-wide leading-4.5 uppercase px-3 pt-4 pb-3"
        >
          {{ group.label }}
        </div>
        <lf-filter-select-option
          v-for="(option, oi) of group.options"
          :key="oi"
          v-model="form.value"
          :value="option.value"
          data-qa="filter-select-option"
          :data-qa-value="option.value"
        >
          {{ option.label }}
        </lf-filter-select-option>
      </template>
      <div v-if="isEmpty" class="text-gray-400 text-xs italic pb-1 pt-3 px-2">
        No results found
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue';
import type {
  SelectFilterConfig,
  SelectFilterOptions,
  SelectFilterValue,
} from '@/shared/modules/filters/types/filterTypes/SelectFilterConfig';
import { required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import LfFilterIncludeSwitch from '@/shared/modules/filters/components/partials/FilterIncludeSwitch.vue';
import LfFilterSelectOption from '@/shared/modules/filters/components/partials/select/FilterSelectOption.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  modelValue: SelectFilterValue,
  config: SelectFilterConfig
} & SelectFilterOptions>();

const emit = defineEmits<{(e: 'update:modelValue', value: SelectFilterValue): void}>();

const search = ref('');

const allOptions = computed(() => props.options.map((g) => g.options).flat());

const filteredOptions = computed(() => props.options.map((g) => ({
  ...g,
  options: g.options.filter((o) => o.label.toLowerCase().includes(search.value.toLowerCase())),
})));

const form = computed({
  get: () => props.modelValue,
  set: (value: SelectFilterValue) => emit('update:modelValue', value),
});

const isEmpty = computed(() => filteredOptions.value.map((g) => g.options).flat().length === 0);

const defaultForm: SelectFilterValue = {
  value: '',
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
  name: 'LfSelectFilter',
};
</script>
