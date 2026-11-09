<template>
  <lf-svg
    :class="svgClass"
    :color="color"
    :name="platform"
  />
</template>

<script setup lang="ts">
import LfSvg from '@/shared/svg/svg.vue';
import { computed } from 'vue';

defineOptions({
  name: 'AppPlatformSvg',
});

const props = defineProps<{
    platform: string;
    size?: string;
    color?: string;
}>();

const color = computed(() => props.color || undefined);

const svgClass = computed(() => {
  if (props.size === 'large') {
    return 'max-w-[20px] h-5';
  }

  return 'max-w-[16px] h-4';
});
</script>
