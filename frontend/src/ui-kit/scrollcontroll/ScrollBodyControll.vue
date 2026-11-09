<template>
  <slot />
</template>

<script setup lang="ts">
import { onBeforeUnmount, onMounted } from 'vue';

defineOptions({
  name: 'LfScrollBodyControll',
});

const emit = defineEmits<{(e: 'bottom'): void }>();

let mainPageWrapper: HTMLElement | null = null;

function handleScroll() {
  if (mainPageWrapper) {
    const isBottom = Math.abs(mainPageWrapper.scrollHeight - mainPageWrapper.scrollTop - mainPageWrapper.clientHeight) < 1;
    if (isBottom) {
      emit('bottom');
    }
  }
}

onMounted(() => {
  mainPageWrapper = document.getElementById('main-page-wrapper');
  if (mainPageWrapper) {
    mainPageWrapper.addEventListener('scroll', handleScroll);
  }
});

onBeforeUnmount(() => {
  if (mainPageWrapper) {
    mainPageWrapper.removeEventListener('scroll', handleScroll);
  }
});
</script>
