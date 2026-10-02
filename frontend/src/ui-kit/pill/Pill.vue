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

const props = withDefaults(defineProps<{
  type?: PillType;
}>(), {
  type: 'primary',
});
</script>

<script lang="ts">
export default {
  name: 'LfPill',
};
</script>
