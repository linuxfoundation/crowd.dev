<template>
  <i :class="`fa-${props.name} fa-${props.type}`" :style="sizeStyle" class="c-icon" />
</template>

<script lang="ts" setup>
import { computed } from 'vue';

defineOptions({
  name: 'LfIcon',
});

const props = withDefaults(defineProps<{
  name: string,
  type?: string,
  size?: number,
}>(), {
  type: 'light',
  size: undefined,
});

const sizeStyle = computed(() => {
  if (!props.size) {
    return undefined;
  }
  return {
    '--lf-icon-size': `${(props.size / 16)}rem`,
  };
});
</script>
