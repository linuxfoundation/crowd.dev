<template>
  <div
    v-if="errors.length > 0"
    class="el-form-item__error"
    :class="errorClass"
  >
    <div v-if="errorMessage(errors[0]).length > 0" class="error-msg flex items-center">
      <i :class="errorIcon" class="mr-1 text-base" />{{ errorMessage(errors[0]) }}
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, unref } from 'vue';
import type { ErrorObject } from '@vuelidate/core';

defineOptions({ name: 'AppFormErrors' });

const props = withDefaults(defineProps<{
  validation?: { $errors?: ErrorObject[] };
  errorMessages?: Record<string, string>;
  hideDefault?: boolean;
  errorIcon?: string;
  errorClass?: string;
}>(), {
  validation: () => ({}),
  errorMessages: () => ({}),
  hideDefault: false,
  errorIcon: '',
  errorClass: '',
});

const errors = computed(() => props.validation?.$errors || []);

const errorMessage = (error: ErrorObject) => {
  const prop = `${error.$property}-${error.$validator}`;
  if (
    props.errorMessages
    && props.errorMessages[prop]
  ) {
    return props.errorMessages[prop];
  }
  if (!props.hideDefault) {
    return unref(error.$message);
  }
  return '';
};
</script>
