<template>
  <div v-if="form">
    <lf-filter-include-switch v-if="!props.hideIncludeSwitch" v-model="form.include" />
    <div class="border-b border-gray-100 px-2 py-1">
      <el-input
        v-model="search"
        placeholder="Search..."
        class="filter-dropdown-search"
        @update:model-value="searchOptions"
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
      <lf-filter-select-option
        v-for="(option, oi) of filteredOptions"
        :key="oi"
        :value="option.value"
        :model-value="form.value"
        data-qa="filter-select-option"
        :data-qa-value="option.value"
        @update:model-value="selectItem(option)"
      >
        <div v-if="option.prefix" v-html="$sanitize(option.prefix)" />
        <div>
          <p class="mb-0 leading-5">
            {{ option.label }}
          </p>
          <p
            v-if="option.description"
            class="text-2xs text-gray-500 leading-5"
          >
            {{ option.description }}
          </p>
        </div>
      </lf-filter-select-option>
      <div v-if="search.length && filteredOptions.length === 0" class="text-gray-400 text-xs italic pb-1 pt-3 px-2">
        No results found
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import {
  computed, onMounted, ref, watch,
} from 'vue';
import { required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import LfFilterIncludeSwitch from '@/shared/modules/filters/components/partials/FilterIncludeSwitch.vue';
import LfFilterSelectOption from '@/shared/modules/filters/components/partials/select/FilterSelectOption.vue';
import type {
  SelectAsyncFilterConfig, SelectAsyncFilterOption, SelectAsyncFilterOptions,
  SelectAsyncFilterValue,
} from '@/shared/modules/filters/types/filterTypes/SelectAsyncFilterConfig';
import type { SelectFilterValue } from '@/shared/modules/filters/types/filterTypes/SelectFilterConfig';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  modelValue: SelectAsyncFilterValue,
  data: any,
  config: SelectAsyncFilterConfig
} & SelectAsyncFilterOptions>();

const emit = defineEmits<{(e: 'update:modelValue', value: SelectAsyncFilterValue): void, (e: 'update:data', value: any): void}>();

const form = computed({
  get: () => props.modelValue,
  set: (value: SelectFilterValue) => emit('update:modelValue', value),
});

const data = computed({
  get: () => props.data,
  set: (value: any) => emit('update:data', value),
});

const defaultForm: SelectAsyncFilterValue = {
  value: '',
  include: true,
};

const rules: any = {
  value: {
    required,
  },
};

useVuelidate(rules, form);

watch(() => props.modelValue.value, (value?: string) => {
  if (!!value && value !== data.value.selected?.value) {
    props.remotePopulateItems(value)
      .then((populated) => {
        data.value.selected = populated;
      });
  }
}, { immediate: true });

const loading = ref<boolean>(false);
const filteredOptions = ref<SelectAsyncFilterOption[]>([]);
const search = ref<string>('');
const searchOptions = (query: string) => {
  loading.value = true;
  props.remoteMethod(query)
    .then((options) => {
      filteredOptions.value = options;
    })
    .finally(() => {
      loading.value = false;
    });
};

const selectItem = (value: any) => {
  data.value.selected = value;
  emit('update:modelValue', {
    ...props.modelValue,
    value: value.value,
  });
};

onMounted(() => {
  searchOptions('');
  emit('update:modelValue', {
    ...defaultForm,
    ...form.value,
  });
  if (!data.value.selected) {
    data.value.selected = '';
  }
});
</script>

<script lang="ts">
export default {
  name: 'LfSelectAsyncFilter',
};
</script>
