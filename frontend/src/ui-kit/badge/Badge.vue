<template>
  <div
    class="c-badge"
    :class="[
      `c-badge--${props.size}`,
      `c-badge--${props.type}`,
    ]"
  >
    <slot />
  </div>
</template>

<script setup lang="ts">
import type { BadgeSize } from '@/ui-kit/badge/types/BadgeSize';
import type { BadgeType } from '@/ui-kit/badge/types/BadgeType';

const props = withDefaults(defineProps<{
  size?: BadgeSize,
  type?: BadgeType,
}>(), {
  size: 'medium',
  type: 'primary',
});
</script>

<script lang="ts">
export default {
  name: 'LfBadge',
};
</script>
