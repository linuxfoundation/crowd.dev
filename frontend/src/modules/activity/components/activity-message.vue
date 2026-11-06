<template>
  <div
    class="activity-message"
    v-html="$sanitize(activityMessage)"
  />
</template>

<script setup lang="ts">
import { computed } from 'vue';
import type { Activity } from '@/shared/modules/activity/types/Activity';

defineOptions({
  name: 'AppActivityMessage',
});

const props = withDefaults(defineProps<{
  activity: Activity;
  type?: keyof Activity['display'];
}>(), {
  type: 'default',
});

const activityMessage = computed(() => {
  if (
    props.activity?.display
    && props.type in props.activity.display
  ) {
    return props.activity.display[props.type];
  }
  return '';
});

</script>

<style lang="scss">
.activity-message {
  * {
    @apply inline-block align-bottom;
  }

  a,
  span {
    @apply text-primary-500;

    &.gray {
      @apply text-gray-500;
    }

    &:not(.notruncate) {
      @apply truncate max-w-2xs;
    }
  }

  img {
    @apply h-4 w-auto;
  }
}
</style>
