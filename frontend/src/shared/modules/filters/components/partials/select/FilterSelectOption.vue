<template>
  <div
    :class="selected ? 'is-selected' : ''"
    v-bind="$attrs"
    class="filter-select-option"
    @click="selectOption()"
  >
    <slot />
    <lf-icon v-if="selected" name="check" :size="16" class="text-primary-600 absolute right-0 mr-4" />
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

defineOptions({
  name: 'LfFilterSelectOption',
});

const props = defineProps<{
  modelValue: boolean | string,
  value: boolean | string,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean | string): void}>();

const selected = computed(() => props.modelValue === props.value);

const selectOption = () => {
  emit('update:modelValue', props.value);
};

</script>

<style lang="scss" scoped>
.filter-select-option{
  @apply flex items-center text-black px-3 py-2.5 text-xs leading-5 cursor-pointer relative transition;
  border-radius: 4px;

  &:not(:last-of-type) {
    @apply mb-1;
  }

  i {
    @apply mr-2;
  }

  i:not(.fa-trash-can) {
    @apply text-gray-400;
  }

  &:focus,
  &:not(.is-disabled):hover,
  &:not(.is-disabled):focus {
    @apply text-black bg-gray-50;
  }

  &.is-disabled {
    @apply cursor-not-allowed;
  }

  &.is-selected,
  &:focus.is-selected {
    @apply relative bg-primary-50;
    i {
      @apply mr-3 text-primary-600;
    }

    &:hover{
      @apply bg-primary-50;
    }
  }
}
</style>
