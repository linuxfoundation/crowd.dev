<template>
  <label
    class="c-checkbox"
    :class="[
      `c-checkbox--${props.size}`,
      props.multiple && 'c-checkbox--multiple',
    ]"
  >
    <input v-model="checked" :indeterminate="indeterminate" type="checkbox" :value="props.value" :disabled="props.disabled">
    <span class="flex flex-col">
      <slot />
    </span>
  </label>
</template>

<script setup lang="ts">
import type { CheckboxSize } from '@/ui-kit/checkbox/types/CheckboxSize';
import { computed, withDefaults } from 'vue';

const props = withDefaults(defineProps<{
  size?: CheckboxSize,
  modelValue: string | boolean | string[],
  value?: string | boolean,
  disabled?: boolean,
  multiple?: boolean,
  indeterminate?: boolean,
}>(), {
  size: 'medium',
  value: true,
  disabled: false,
  multiple: false,
  indeterminate: false,
});

const emit = defineEmits<{(e: 'update:modelValue', value: string | boolean | string[]): any}>();

const checked = computed<string | boolean | string[]>({
  get() {
    return props.modelValue;
  },
  set(val: string | boolean | string[]) {
    emit('update:modelValue', val);
  },
});
</script>

<script lang="ts">
export default {
  name: 'LfCheckbox',
};
</script>
