<!--
Copyright (c) 2025 The Linux Foundation and each contributor.
SPDX-License-Identifier: MIT
-->
<template>
  <div
    ref="loadMore"
    class="flex flex-row gap-2 items-center justify-center w-full"
  >
    <span>
      <lf-spinner
        size="16px"
        class="text-brand-300"
      />
    </span>
    <span class="text-sm text-brand-300 text-sm font-semibold">
      {{ text }}
    </span>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, watch } from 'vue';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';

const emit = defineEmits<{(e: 'loadMore'): void }>();

const props = defineProps<{
  text: string;
  isFetchingNextPage: boolean;
}>();

const loadMore = ref(null);

const options = {
  root: null,
  rootMargin: '0px',
  threshold: 0,
};

const handleIntersectCallback = (entries: IntersectionObserverEntry[]) => {
  entries.forEach((entry) => {
    if (entry.isIntersecting) {
      emit('loadMore');
    }
  });
};

const isLoadMoreVisible = () => {
  if (!loadMore.value) {
    return false;
  }

  return isElementVisible(loadMore.value as HTMLElement);
};

const isElementVisible = (element: HTMLElement) => {
  const rect = element.getBoundingClientRect();
  const windowHeight = window.innerHeight || document.documentElement.clientHeight;
  return rect.top >= 0 && rect.bottom <= windowHeight;
};

onMounted(() => {
  if (loadMore.value) {
    const observer = new IntersectionObserver(handleIntersectCallback, options);
    observer.observe(loadMore.value);
  }
});

watch(
  () => props.isFetchingNextPage,
  (newVal: boolean) => {
    if (!newVal) {
      // check if the load more is visible
      if (isLoadMoreVisible()) {
        emit('loadMore');
      }
    }
  },
);
</script>

<script lang="ts">
export default {
  name: 'AppLfLoadMore',
};
</script>
