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
const props = withDefaults(defineProps<{
  width?: string;
}>(), {
  width: '1rem',
});
</script>

<script lang="ts">
export default {
  name: 'LfTimeline',
};
</script>
