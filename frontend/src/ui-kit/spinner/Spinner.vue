<template>
  <div class="c-spinner" :style="{ '--lf-spinner-size': props.size }" />
</template>

<script setup lang="ts">
defineOptions({
  name: 'LfSpinner',
});

const props = withDefaults(defineProps<{
  size?: string,
}>(), {
  size: '2rem',
});

</script>
