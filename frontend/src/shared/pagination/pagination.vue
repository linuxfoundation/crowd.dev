<template>
  <div class="pagination-wrapper flex grow justify-between">
    <el-pagination
      :current-page="currentPage || 1"
      :page-size="pageSize"
      :page-count="Math.ceil(total / pageSize || 1)"
      :prev-icon="ArrowPrevIcon"
      :next-icon="ArrowNextIcon"
      hide-on-single-page
      layout="prev, pager, next"
      @current-change="
        (currentPage: number) =>
          emit('changeCurrentPage', currentPage)
      "
    />

    <app-pagination-sorter
      v-if="!hideSorting"
      :page-size="pageSize"
      :total="total"
      :current-page="currentPage"
      :module="module"
      @change-sorter="
        (pageSize: number) => emit('changePageSize', pageSize)
      "
    />
  </div>
</template>

<script setup lang="ts">
import { h } from 'vue';

defineOptions({ name: 'AppPagination' });

withDefaults(defineProps<{
  currentPage: number;
  pageSize: number;
  total: number;
  hideSorting?: boolean;
  module?: string;
}>(), {
  hideSorting: false,
  module: '',
});

const emit = defineEmits<{(e: 'changeCurrentPage', page: number): void;
  (e: 'changePageSize', pageSize: number): void;
}>();

const ArrowPrevIcon = h(
  'i', // type
  {
    class: 'fa-chevron-left fa-light text-lg leading-none',
  }, // props
  [],
);

const ArrowNextIcon = h(
  'i', // type
  {
    class: 'fa-chevron-right fa-light text-lg leading-none',
  }, // props
  [],
);
</script>

<style lang="scss">
.pagination-wrapper .el-pagination {
  @apply flex gap-2 m-0 p-0;

  // Reset button margins
  button {
    @apply m-0;
  }

  // Previous and next buttons style
  .btn-prev,
  .btn-next {
    @apply rounded text-gray-500;

    &:not([disabled]):hover {
      @apply bg-gray-100;
    }

    &[disabled] {
      @apply text-gray-400;
    }

    &-is-first,
    &-is-last {
      @apply cursor-not-allowed;
    }
  }

  .el-icon {
    @apply h-5;
  }

  // Number buttons style
  ul.el-pager {
    @apply flex gap-2;

    li.number {
      @apply font-normal text-gray-600 m-0 rounded;

      &:not(.is-active):hover {
        @apply bg-gray-100;
      }

      &.is-active {
        @apply bg-primary-500 border-primary-500 text-white;
      }
    }
  }
}
</style>
