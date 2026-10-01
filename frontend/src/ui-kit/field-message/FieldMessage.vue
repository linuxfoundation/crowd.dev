<template>
  <div class="c-field-message" :class="`c-field-message--${props.type}`" v-bind="$attrs">
    <span v-if="$slots.icon || iconClass" class="c-field-message__icon">
      <slot name="icon">
        <lf-icon
          v-if="!props.hideIcon"
          :name="iconClass || ''"
        />
      </slot>
    </span>

    <slot />
  </div>
</template>

<script setup lang="ts">
import type { FieldMessageType } from '@/ui-kit/field-message/types/FieldMessageType';
import { computed } from 'vue';
import { fieldMessageTypeData } from '@/ui-kit/field-message/constants/fieldMessageTypeData';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = withDefaults(defineProps<{
  type?: FieldMessageType,
  hideIcon?: boolean,
}>(), {
  type: 'error',
  hideIcon: false,
});

const iconClass = computed(() => {
  const { icon } = fieldMessageTypeData[props.type];
  if (icon) {
    return icon;
  }
  return null;
});
</script>

<script lang="ts">
export default {
  name: 'LfFieldMessage',
};
</script>
