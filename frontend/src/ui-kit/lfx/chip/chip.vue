<!--
Copyright (c) 2025 The Linux Foundation and each contributor.
SPDX-License-Identifier: MIT
-->
<template>
  <div
    v-if="isVisible"
    :class="`p-chip p-chip-size-${props.size} p-chip-type-${props.type}`"
    role="alert"
  >
    <slot />
    <i
      v-if="props.removable"
      class="p-chip-remove-icon fa-solid fa-circle-xmark"
      @click="dismiss"
    />
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue';
import type { ChipSize, ChipType } from './types/chip.types';

defineOptions({
  name: 'LfxChip',
});

const props = withDefaults(
  defineProps<{
    size?: ChipSize;
    type?: ChipType;
    removable?: boolean;
  }>(),
  {
    type: 'default',
    size: 'default',
    removable: false,
  },
);
const emit = defineEmits<{(e: 'dismissed'): void }>();

const isVisible = ref(true);

const dismiss = () => {
  isVisible.value = false;
  emit('dismissed');
};
</script>
