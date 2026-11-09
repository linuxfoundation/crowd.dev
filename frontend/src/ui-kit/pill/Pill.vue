<template>
  <span
    class="c-pill"
    :class="[
      `c-pill--${props.type}`,
    ]"
  >
    <slot />
  </span>
</template>

<script lang="ts" setup>
import type { PillType } from '@/ui-kit/pill/types/PillType';

defineOptions({
  name: 'LfPill',
});

const props = withDefaults(defineProps<{
  type?: PillType;
}>(), {
  type: 'primary',
});
</script>
