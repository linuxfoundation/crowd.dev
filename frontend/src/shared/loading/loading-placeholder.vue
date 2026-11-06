<template>
  <div
    class="loading-placeholder"
    v-bind="$attrs"
    :style="{
      height: height,
      width: width,
      'border-radius': radius,
    }"
  />
</template>

<script setup lang="ts">
defineOptions({ name: 'AppLoading' });

withDefaults(defineProps<{
  height?: string;
  width?: string;
  radius?: string;
}>(), {
  height: '100px',
  width: '100%',
  radius: '0px',
});
</script>

<style scoped lang="scss">
@keyframes shimmer {
  100% {
    transform: translateX(100%);
  }
}

.loading-placeholder {
  @apply bg-gray-100;
  display: block;
  position: relative;
  overflow: hidden;

  &:after {
    position: absolute;
    top: 0;
    right: 0;
    bottom: 0;
    left: 0;
    transform: translateX(-100%);
    background-image: linear-gradient(
      90deg,
      rgba(255, 255, 255, 0) 0%,
      rgba(255, 255, 255, 0.2) 20.24%,
      rgba(255, 255, 255, 0.5) 42.57%,
      rgba(255, 255, 255, 0) 66.35%
    );
    animation: shimmer 1.3s infinite;
    content: '';
  }
}
</style>
