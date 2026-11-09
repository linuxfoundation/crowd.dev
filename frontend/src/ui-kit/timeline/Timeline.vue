<template>
  <div
    class="c-timeline"
    :style="{
      '--lf-timeline-width': props.width,
    }"
  >
    <slot />
  </div>
</template>

<script lang="ts" setup>
defineOptions({
  name: 'LfTimeline',
});

const props = withDefaults(defineProps<{
  width?: string;
}>(), {
  width: '1rem',
});
</script>
