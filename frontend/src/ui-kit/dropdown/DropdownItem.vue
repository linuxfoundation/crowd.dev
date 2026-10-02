<template>
  <div
    class="c-dropdown__item"
    :class="[
      props.type && `c-dropdown__item--${type}`,
      {
        'is-disabled': props.disabled,
        'is-selected': props.selected,
      },
    ]"
    @click="handleClick"
  >
    <slot />
  </div>
</template>

<script lang="ts" setup>
import type { DropdownItemType } from '@/ui-kit/dropdown/types/DropdownItemType';

const props = withDefaults(defineProps<{
  disabled?: boolean
  selected?: boolean
  type?: DropdownItemType,
}>(), {
  disabled: false,
  selected: false,
  type: 'regular',
});

const emit = defineEmits<{(e: 'click'): void}>();

const handleClick = (event: MouseEvent) => {
  if (props.disabled) {
    event.preventDefault();
    return;
  }
  emit('click');
};

</script>

<script lang="ts">
export default {
  name: 'LfDropdownItem',
};
</script>
