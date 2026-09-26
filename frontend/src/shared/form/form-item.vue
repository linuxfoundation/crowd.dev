<template>
  <div
    class="el-form-item"
    :class="{
      'is-error': enabledError(errors),
    }"
    v-bind="$attrs"
  >
    <div class="el-form-item__content flex flex-col !items-start">
      <label
        v-if="label"
        :for="formId"
        class="text-xs mb-1 font-semibold leading-5 block text-gray-900"
      >{{ label }}
        <span
          v-if="required"
          class="text-red-500"
        >*</span></label>

      <div class="w-full">
        <slot />
      </div>
      <div
        v-if="showError && errors.length > 0"
        class="el-form-item__error"
        :class="errorClass"
      >
        <div class="error-msg">
          <i :class="errorIcon" class="mr-1 text-base" />{{ errorMessage(errors[0]) }}
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import type { ErrorObject } from '@vuelidate/core';

defineOptions({ name: 'AppFormItem' });

const props = withDefaults(defineProps<{
  validation?: { $errors?: ErrorObject[] };
  label?: string;
  formId?: string;
  required?: boolean;
  errorMessages?: Record<string, string>;
  filterErrors?: string[] | null;
  showError?: boolean;
  errorIcon?: string;
  errorClass?: string;
}>(), {
  validation: () => ({}),
  label: '',
  formId: '',
  required: false,
  errorMessages: () => ({}),
  filterErrors: () => null,
  showError: true,
  errorIcon: '',
  errorClass: '',
});

defineSlots<{
  default?:() => unknown;
}>();

const errors = computed(() => props.validation?.$errors || []);

const enabledError = (errors: ErrorObject[]): boolean => {
  if (props.filterErrors && props.filterErrors.length > 0 && errors.length > 0) {
    return props.filterErrors.includes(errors[0].$validator);
  }
  return errors.length > 0;
};

const errorMessage = (error: ErrorObject): ErrorObject['$message'] => {
  if (
    props.errorMessages
    && props.errorMessages[error.$validator]
  ) {
    return props.errorMessages[error.$validator];
  }
  return error.$message;
};
</script>
