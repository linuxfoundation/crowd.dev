<!--
Copyright (c) 2025 The Linux Foundation and each contributor.
SPDX-License-Identifier: MIT
-->
<template>
  <div
    class="lfx-c-dropdown__selector"
    :class="[`lfx-c-dropdown__selector--${props.size}`, `lfx-c-dropdown__selector--${props.type}`]"
  >
    <slot />
    <slot name="append">
      <lf-icon
        name="angle-down"
        :size="12"
        class="text-neutral-500"
      />
    </slot>
  </div>
</template>

<script lang="ts" setup>
import LfIcon from '@/ui-kit/icon/Icon.vue';

defineOptions({
  name: 'LfxDropdownSelector',
});

const props = withDefaults(
  defineProps<{
    size?: 'medium' | 'small';
    type?: 'transparent' | 'filled';
  }>(),
  {
    size: 'medium',
    type: 'transparent',
  },
);
</script>
