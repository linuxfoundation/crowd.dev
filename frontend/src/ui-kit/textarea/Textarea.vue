<template>
  <div class="c-input" :class="['c-input', { 'is-invalid': props.invalid }]">
    <textarea v-model="value" :placeholder="props.placeholder" :disabled="props.disabled" class="h-full" v-bind="$attrs" />
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';

const props = withDefaults(defineProps<{
  modelValue: string | number,
  placeholder?: string,
  disabled?: boolean,
  invalid?: boolean,
}>(), {
  placeholder: '',
  disabled: false,
  invalid: false,
});

const emit = defineEmits<{(e: 'update:modelValue', value: string | number): void}>();

const value = computed<string | number>({
  get() {
    return props.modelValue;
  },
  set(val: string | number) {
    emit('update:modelValue', val);
  },
});
</script>

<script lang="ts">
export default {
  name: 'LfTextarea',
};
</script>
