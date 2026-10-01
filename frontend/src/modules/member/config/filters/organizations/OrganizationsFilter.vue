<template>
  <div v-if="props.modelValue">
    <cr-filter-include-switch
      v-if="!props.hideIncludeSwitch"
      v-model="include"
    />
    <div class="px-4 pt-3">
      <el-select
        :model-value="data.selected || []"
        multiple
        remote
        filterable
        :reserve-keyword="false"
        placeholder="Select options"
        :remote-method="searchOptions"
        :teleported="false"
        class="filter-multiselect"
        popper-class="filter-multiselect-popper"
        :loading="loading"
        no-data-text=""
        @update:model-value="updateSelected($event)"
      >
        <el-option
          v-for="option of filteredOptions"
          :key="option.value"
          :label="option.label"
          :value="option"
          class="!h-auto !min-h-10 !py-2.5"
        >
          <lf-checkbox :model-value="form.value.includes(option.value)">
            <div v-if="option.prefix" v-html="$sanitize(option.prefix)" />
            <div>
              <div class="flex items-center gap-1">
                <p class="mb-0 leading-5">
                  {{ option.label }}
                </p>
                <lf-organization-lf-member-tag
                  :organization="option"
                  only-show-icon
                />
              </div>
              <p
                v-if="option.description"
                class="text-2xs text-gray-500 leading-5"
              >
                {{ option.description }}
              </p>
            </div>
          </lf-checkbox>
        </el-option>
      </el-select>
    </div>
  </div>
</template>

<script setup lang="ts">
import {
  computed, onMounted, ref, watch,
} from 'vue';
import { required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import CrFilterIncludeSwitch from '@/shared/modules/filters/components/partials/FilterIncludeSwitch.vue';
import type {
  MultiSelectAsyncFilterConfig,
  MultiSelectAsyncFilterOption,
  MultiSelectAsyncFilterOptions,
  MultiSelectAsyncFilterValue,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectAsyncFilterConfig';
import type { MultiSelectFilterValue } from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import LfOrganizationLfMemberTag from '@/modules/organization/components/lf-member/organization-lf-member-tag.vue';
import LfCheckbox from '@/ui-kit/checkbox/Checkbox.vue';

const props = defineProps<{
    modelValue: MultiSelectAsyncFilterValue,
    data: any,
    config: MultiSelectAsyncFilterConfig
  } & MultiSelectAsyncFilterOptions>();

const emit = defineEmits<{(e: 'update:modelValue', value: MultiSelectAsyncFilterValue): void, (e: 'update:data', value: any): void}>();

const form = computed({
  get: () => props.modelValue,
  set: (value: MultiSelectFilterValue) => emit('update:modelValue', value),
});

const data = computed({
  get: () => props.data,
  set: (value: any) => emit('update:data', value),
});

const defaultForm: MultiSelectAsyncFilterValue = {
  value: [],
  include: true,
};

const rules: any = {
  value: {
    required,
  },
};

useVuelidate(rules, form);

const include = computed<boolean>({
  get() {
    return props.modelValue.include;
  },
  set(value: boolean) {
    emit('update:modelValue', {
      ...props.modelValue,
      include: value,
    });
  },
});

watch(() => props.modelValue.value, (value?: string[]) => {
  if (value?.length !== data.value.selected?.length) {
    props.remotePopulateItems(value || [])
      .then((options) => {
        data.value.selected = options;
      });
  }
}, { immediate: true });

const loading = ref<boolean>(false);
const filteredOptions = ref<MultiSelectAsyncFilterOption[]>([]);

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

const updateSelected = (value: any[]) => {
  data.value.selected = value;
  emit('update:modelValue', {
    ...props.modelValue,
    value: value.map((v) => v.value),
  });
};

onMounted(() => {
  searchOptions('');
  emit('update:modelValue', {
    ...defaultForm,
    ...form.value,
  });
  if (!data.value.selected) {
    data.value.selected = [];
  }
});
</script>

<script lang="ts">
export default {
  name: 'LfMultiSelectAsyncFilter',
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
