<template>
  <img
    v-lazy="src"
    lazy="loading"
    class="object-cover object-center"
    :alt="alt"
  />
</template>

<script setup lang="ts">
defineOptions({ name: 'AppImage' });

defineProps<{
  src: string;
  alt: string;
}>();
</script>

<style lang="scss" scoped>
img[lazy='loading'] {
  @apply w-full bg-gray-200 animate-pulse;
}
</style>
