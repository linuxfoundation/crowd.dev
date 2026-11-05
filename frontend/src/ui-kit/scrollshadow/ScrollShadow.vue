<template>
  <div class="relative">
    <div ref="scrollContainer" class="c-scrollshadow" v-bind="$attrs" @scroll="handleScroll">
      <slot />
    </div>
    <div class="c-scrollshadow__top" :class="{ 'is-visible': showTopShadow }" />
    <div class="c-scrollshadow__bottom" :class="{ 'is-visible': showBottomShadow }" />
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue';

const scrollContainer = ref<HTMLElement | null>(null);
const showTopShadow = ref<boolean>(true);
const showBottomShadow = ref<boolean>(true);

const handleScroll = () => {
  const scrollTop = scrollContainer.value?.scrollTop || 0;
  const clientHeight = scrollContainer.value?.clientHeight || 0;
  const scrollHeight = scrollContainer.value?.scrollHeight || 0;
  showTopShadow.value = scrollTop > 0;
  showBottomShadow.value = scrollTop + clientHeight < scrollHeight;
};

onMounted(() => {
  handleScroll();
});
</script>

<script lang="ts">
export default {
  name: 'LfScrollShadow',
};
</script>
