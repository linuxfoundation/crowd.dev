<template>
  <div ref="infinitePaginationWrapper">
    <slot v-if="useSlot && isLoading" />
    <template v-else>
      <div v-if="isLoading" class="text-sm text-gray-500 text-center">
        Loading...
      </div>
    </template>
  </div>
</template>

<script lang="ts" setup>
import { onMounted, ref } from 'vue';

const props = defineProps({
  currentPage: {
    type: Number,
    required: true,
  },
  pageSize: {
    type: Number,
    required: true,
  },
  total: {
    type: Number,
    required: true,
  },
  isLoading: {
    type: Boolean,
    required: true,
  },
  useSlot: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits([
  'loadMore',
]);

const infinitePaginationWrapper = ref();

onMounted(() => {
  observeSections();
});

const observeSections = () => {
  const options = {
    rootMargin: '0px 0px',
    threshold: 0,
  };
  const observer = new IntersectionObserver(sectionObserveHandler, options);

  observer.observe(infinitePaginationWrapper.value as Element);
};

const sectionObserveHandler = (entries: IntersectionObserverEntry[]) => {
  entries.forEach((entry) => {
    if (entry.intersectionRatio > 0 && props.total > props.currentPage * props.pageSize) {
      emit('loadMore', props.currentPage + 1);
    }
  });
};

</script>
<script lang="ts">
export default {
  name: 'InfinitePagination',
};
</script>
