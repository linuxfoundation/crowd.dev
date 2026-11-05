<template>
  <div v-for="error of errors.slice(0, 1)" :key="error.$property">
    <lf-field-message v-if="errorMessage(error)" v-bind="$attrs">
      {{ errorMessage(error) }}
      <template v-if="$slots.icon" #icon>
        <slot name="icon" />
      </template>
    </lf-field-message>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import type { ErrorObject } from '@vuelidate/core';
import LfFieldMessage from '@/ui-kit/field-message/FieldMessage.vue';

const props = withDefaults(defineProps<{
  validation: { $errors: ErrorObject[] },
  errorMessages?: Record<string, string>,
  hideDefault?: boolean,
}>(), {
  errorMessages: () => ({}),
});

const errors = computed(() => (props.validation?.$errors || []));

const errorMessage = (error: ErrorObject) => {
  const prop = error.$validator;
  if (
    props.errorMessages
      && props.errorMessages[prop] !== undefined
  ) {
    return props.errorMessages[prop];
  }
  if (!props.hideDefault) {
    return error.$message;
  }
  return '';
};
</script>

<script lang="ts">
export default {
  name: 'LfFieldMessages',
};
</script>
