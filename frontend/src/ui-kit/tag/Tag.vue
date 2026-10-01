<template>
  <div
    class="c-tag"
    :class="[
      `c-tag--${props.size}`,
      `c-tag--${props.type}`,
      props.closeable ? 'c-tag--closeable' : '',
      props.rounded ? 'c-tag--rounded' : '',
    ]"
    :style="props.bgColor ? `background-color: ${props.bgColor}` : ''"
  >
    <span class="c-tag-content">
      <slot />
    </span>
    <lf-icon class="c-tag-close-icon" name="xmark" :size="14" @click="emit('close')" />
  </div>
</template>

<script setup lang="ts">
import LfIcon from '@/ui-kit/icon/Icon.vue';

import { defineProps } from 'vue';
import type { TagSize } from './types/TagSize';
import type { TagType } from './types/TagType';

const props = withDefaults(defineProps<{
    size: TagSize,
    type: TagType,
    closeable: boolean,
    rounded: boolean,
    bgColor: string,
}>(), {
  size: 'small',
  type: 'primary',
  closeable: false,
  rounded: false,
  bgColor: undefined,
});

const emit = defineEmits<{(e: 'close'): void;
}>();
</script>

<script lang="ts">
export default {
  name: 'LfTag',
};
</script>
