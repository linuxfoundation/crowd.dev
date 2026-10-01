<template>
  <label
    class="c-switch"
    :class="[
      `c-switch--${props.size}`,
    ]"
  >
    <span v-if="$slots.inactive" class="flex flex-col">
      <slot name="inactive" />
    </span>
    <input v-model="checked" type="checkbox" :value="props.value" :disabled="props.disabled">
    <span v-if="$slots.default" class="flex flex-col">
      <slot />
    </span>
  </label>
</template>

<script setup lang="ts">
import type { CheckboxSize } from '@/ui-kit/checkbox/types/CheckboxSize';
import { computed, withDefaults } from 'vue';

const props = withDefaults(defineProps<{
  size?: CheckboxSize,
  modelValue: string | boolean,
  value?: string | boolean,
  disabled?: boolean,
}>(), {
  size: 'small',
  value: true,
  disabled: false,
});

const emit = defineEmits<{(e: 'update:modelValue', value: string | boolean): any}>();

const checked = computed<string | boolean>({
  get() {
    return props.modelValue;
  },
  set(val: string | boolean) {
    emit('update:modelValue', val);
  },
});
</script>

<script lang="ts">
export default {
  name: 'LfSwitch',
};
</script>
