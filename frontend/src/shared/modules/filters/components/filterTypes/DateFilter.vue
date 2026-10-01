<template>
  <div v-if="form">
    <div class="p-4">
      <div v-if="!props.datepickerType || props.datepickerType === 'date'">
        <el-select
          v-model="timePicker"
          value-key="id"
          placeholder="Select property"
          class="w-full"
          placement="bottom-end"
          :teleported="false"
        >
          <template #prefix>
            <span class="!text-sm !text-gray-400 leading-5">When:</span>
          </template>
          <el-option
            v-for="time of dateFilterTimePickerOptions"
            :key="time.id"
            :value="time"
            :label="time.label"
          />
          <el-option
            key="custom"
            :value="{
              id: 'custom',
              value: 'custom',
            }"
            label="Custom"
          />
        </el-select>
      </div>
      <div v-if="isCustom" class="flex items-start" :class="!props.datepickerType || props.datepickerType === 'date' ? 'pt-4' : ''">
        <el-select
          v-model="form.operator"
          class="operator-select"
          placement="bottom-start"
          :teleported="false"
        >
          <el-option
            v-for="operator of operators"
            :key="operator.value"
            :value="operator.value"
            :label="operator.label"
          />
        </el-select>
        <el-date-picker
          v-model="form.value"
          :placeholder="isBetween ? 'Select date range' : 'Select date'"
          :value-format="props.dateFormat ?? 'YYYY-MM-DD'"
          :format="props.dateFormat ?? 'YYYY-MM-DD'"
          popper-class="date-picker-popper"
          class="datepicker-input !w-full"
          v-bind="betweenProps"
          :teleported="false"
          @blur="$v.value.$touch"
          @change="$v.value.$touch"
        />
      </div>
      <div class="filter-date-field" data-qa="filter-date-input" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, watch } from 'vue';
import type {
  DateFilterConfig,
  DateFilterOptions,
  DateFilterValue,
} from '@/shared/modules/filters/types/filterTypes/DateFilterConfig';
import { required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import {
  dateFilterOperators,
  dateFilterTimePickerOptions,
  FilterDateOperator,
} from '@/shared/modules/filters/config/constants/date.constants';
import type { FilterTimeOptions } from '../../types/FilterTimeOptions';

const props = defineProps<{
  modelValue: DateFilterValue,
  config: DateFilterConfig
} & DateFilterOptions>();

const emit = defineEmits<{(e: 'update:modelValue', value: DateFilterValue): void}>();

const form = computed({
  get: () => props.modelValue,
  set: (value: DateFilterValue) => emit('update:modelValue', value),
});

const defaultForm: DateFilterValue = {
  operator: FilterDateOperator.EQ,
  value: '',
};

const rules: any = {
  value: {
    required,
  },
  operator: {
    required,
  },
};

const $v = useVuelidate(rules, form);

const timePickerOptionValues = dateFilterTimePickerOptions.map((v) => v.value);

const timePicker = computed({
  get() {
    const option = dateFilterTimePickerOptions.find((o) => o.value === form.value.value && o.operator === form.value.operator);
    if (typeof form.value.value === 'string' && option) {
      return option;
    }

    return {
      id: 'custom',
      value: '',
      label: 'Custom',
      operator: FilterDateOperator.EQ,
    };
  },
  set(optionValue: Partial<FilterTimeOptions>) {
    const option = dateFilterTimePickerOptions.find((o) => o.value === optionValue.value && o.operator === optionValue.operator);

    if (option) {
      form.value = option;
    } else {
      form.value = {
        value: '',
        operator: FilterDateOperator.EQ,
      };
    }
  },
});

const isCustom = computed(() => !(typeof form.value.value === 'string' && timePickerOptionValues.includes(form.value.value)));

const checkIfBetween = (operator: string) => [FilterDateOperator.BETWEEN, FilterDateOperator.NOT_BETWEEN].includes(operator);

const isBetween = computed(() => checkIfBetween(form.value.operator));

const operators = computed(() => {
  if (props.datepickerType === 'year') {
    return dateFilterOperators.filter((o) => !checkIfBetween(o.value));
  }
  return dateFilterOperators;
});

const betweenProps = computed(() => (!isBetween.value
  ? {
    type: props.datepickerType ?? 'date',
  }
  : {
    type: props.datepickerType === 'month' ? 'monthrange' : 'daterange',
    'range-separator': 'To',
    'start-placeholder': 'Start date',
    'end-placeholder': 'End date',
  }));

watch(() => form.value.operator, (operator, previousOperator) => {
  const isPreviousBetweenOperator = checkIfBetween(previousOperator);
  const isCurrentBetweenOperator = checkIfBetween(operator);
  if (isCurrentBetweenOperator !== isPreviousBetweenOperator) {
    form.value.value = '';
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
  name: 'LfDateFilter',
};
</script>

<style lang="scss">
.operator-select{
  .el-input__wrapper{
    @apply rounded-r-none border-r-0 h-10 #{!important};
  }
}

.datepicker-input {
  &, .el-input__wrapper{
    @apply w-full h-10 rounded-l-none h-10 #{!important};
  }
}
</style>
