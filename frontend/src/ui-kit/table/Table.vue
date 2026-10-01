<template>
  <div
    ref="table"
    class="c-table"
    :class="[
      `c-table--${props.type}`,
      {
        'is-scroll-start': isScrolledToStart,
        'is-scroll-end': isScrolledToEnd,
        'hover-enabled': props.showHover,
      },
    ]"
    v-bind="$attrs"
    @scroll="updateScrollPosition"
  >
    <div>
      <table>
        <slot />
      </table>
    </div>
  </div>
</template>

<script lang="ts" setup>
import {
  ref, onMounted, onBeforeUnmount, computed,
} from 'vue';

const props = withDefaults(defineProps<{
  type?: 'regular' | 'bordered',
  showHover?: boolean,
}>(), {
  type: 'regular',
});

const table = ref<HTMLTableElement | null>(null);
const scrollPosition = ref(0);

const updateScrollPosition = () => {
  if (table.value) {
    scrollPosition.value = table.value.scrollLeft;
  }
};

const isScrolledToStart = computed(() => {
  if (table.value) {
    return scrollPosition.value === 0;
  }
  return false;
});

const isScrolledToEnd = computed(() => {
  if (table.value) {
    return scrollPosition.value + table.value.clientWidth >= table.value.scrollWidth;
  }
  return false;
});

onMounted(() => {
  if (table.value) {
    table.value.addEventListener('scroll', updateScrollPosition);
  }
  updateScrollPosition();
});

onBeforeUnmount(() => {
  if (table.value) {
    table.value.removeEventListener('scroll', updateScrollPosition);
  }
});
</script>

<script lang="ts">
export default {
  name: 'LfTable',
};
</script>
