<template>
  <div v-if="form">
    <lf-filter-include-switch v-model="form.include" />
    <div class="p-4 pb-5">
      <lf-filter-inline-select
        v-if="!props.forceOperator"
        v-model="form.operator"
        :prefix="`${props.config.label}:`"
        class="mb-3"
        :options="annualRevenueFilterOperators"
      />
      <div class="flex -mx-1">
        <div class="flex-grow px-1">
          <lf-filter-input
            v-model="form.value"
            type="number"
            min="0"
            step="1"
            prefix="$"
            suffix="M"
            :placeholder="!isBetween ? 'Enter value' : 'Min'"
            data-qa="filter-number-from"
            @blur="$v.value.$touch"
            @change="$v.value.$touch"
          />
        </div>
        <div v-if="isBetween" class="flex-grow px-1">
          <app-form-item
            :validation="$v.valueTo"
            class="mb-0"
            :show-error="false"
            :filter-errors="['minValue']"
          >
            <lf-filter-input
              v-model="form.valueTo"
              type="number"
              placeholder="Max"
              min="0"
              data-qa="filter-number-to"
              prefix="$"
              suffix="M"
              @blur="$v.valueTo.$touch"
              @change="$v.valueTo.$touch"
            />
          </app-form-item>
        </div>
      </div>
      <app-form-errors
        :validation="$v"
        error-icon="fa-circle-exclamation fa-light"
        error-class="relative pt-0"
        :error-messages="{
          'valueTo-minValue': `Number should be higher than “From” field`,
        }"
        :hide-default="true"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, watch } from 'vue';
import type {
  NumberFilterConfig,
  NumberFilterOptions,
  NumberFilterValue,
} from '@/shared/modules/filters/types/filterTypes/NumberFilterConfig';
import useVuelidate from '@vuelidate/core';
import {
  integer, minValue, numeric, required,
} from '@vuelidate/validators';
import {
  FilterNumberOperator,
  numberFilterOperators,
} from '@/shared/modules/filters/config/constants/number.constants';
import LfFilterInput from '@/shared/modules/filters/components/partials/string/FilterInput.vue';
import LfFilterInlineSelect from '@/shared/modules/filters/components/partials/FilterInlineSelect.vue';
import AppFormErrors from '@/shared/form/form-errors.vue';
import AppFormItem from '@/shared/form/form-item.vue';
import LfFilterIncludeSwitch from '@/shared/modules/filters/components/partials/FilterIncludeSwitch.vue';

const props = defineProps<{
  modelValue: NumberFilterValue,
  config: NumberFilterConfig
} & NumberFilterOptions>();

const emit = defineEmits<{(e: 'update:modelValue', value: NumberFilterValue): void}>();

const form = computed<NumberFilterValue>({
  get: () => props.modelValue,
  set: (value: NumberFilterValue) => emit('update:modelValue', value),
});

const defaultForm: NumberFilterValue = {
  value: '',
  include: true,
  operator: FilterNumberOperator.BETWEEN,
};

const rules: any = computed(() => ({
  value: {
    required,
    numeric,
    integer,
    minValue: minValue(0),
  },
  ...(isBetween.value ? {
    valueTo: {
      required,
      numeric,
      integer,
      minValue: minValue((form.value.value) || 0),
    },
  } : {}),
  operator: {
    required,
  },
}));

const isBetween = computed<boolean>(() => ([
  FilterNumberOperator.BETWEEN,
] as FilterNumberOperator[]).includes(form.value.operator));

const annualRevenueFilterOperators = computed(() => numberFilterOperators.filter((operator) => operator.value !== FilterNumberOperator.EQ
      && operator.value !== FilterNumberOperator.NE
      && operator.value !== FilterNumberOperator.NOT_BETWEEN));

const $v = useVuelidate(rules, form);

watch(() => form.value.operator, (operator, previousOperator) => {
  const isPreviousBetweenOperator = ([
    FilterNumberOperator.BETWEEN,
  ] as FilterNumberOperator[]).includes(previousOperator);
  const isCurrentBetweenOperator = ([
    FilterNumberOperator.BETWEEN,
  ] as FilterNumberOperator[]).includes(operator);
  if (!isCurrentBetweenOperator) {
    delete form.value.valueTo;
  } else if (isCurrentBetweenOperator !== isPreviousBetweenOperator) {
    form.value.valueTo = '';
  }
});

onMounted(() => {
  form.value = {
    ...defaultForm,
    ...form.value,
  };
});
</script>

<script lang="ts">
export default {
  name: 'AnnualRevenueFilter',
};
</script>
