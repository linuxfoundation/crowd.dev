<template>
  <label
    class="c-radio"
    :class="[
      `c-radio--${props.size}`,
    ]"
  >
    <input v-model="checked" type="radio" :value="props.value" :disabled="props.disabled" :name="props.name">
    <span class="flex flex-col">
      <slot />
    </span>
  </label>
</template>

<script setup lang="ts">
import type { RadioSize } from '@/ui-kit/radio/types/RadioSize';
import { computed, withDefaults } from 'vue';

const props = withDefaults(defineProps<{
  size?: RadioSize,
  modelValue: string,
  value?: string,
  name?: string,
  disabled?: boolean,
}>(), {
  size: 'medium',
  value: '',
  name: '',
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
  name: 'LfRadio',
};
</script>
