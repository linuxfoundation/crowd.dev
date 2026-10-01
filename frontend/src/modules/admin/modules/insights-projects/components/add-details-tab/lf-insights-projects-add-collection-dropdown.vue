<template>
  <div>
    <el-select
      v-model="cForm.collectionsIds"
      filterable
      multiple
      :filter-method="onSearchInput"
      placeholder="Select collection(s)"
      class="w-full"
      popper-class="collection-infinite-select-dropdown"
    >
      <el-option
        v-for="item in collections"
        :key="item.id"
        :label="item.name"
        :value="item.id"
      />
      <el-option
        v-if="isFetchingNextPage || isPending"
        :key="'loading'"
        label="Loading..."
        value=""
        disabled
      />
    </el-select>
  </div>
</template>

<script setup lang="ts">
import {
  computed,
  nextTick,
  onBeforeUnmount,
  onMounted,
  reactive,
  ref,
  watch,
} from 'vue';
import {
  COLLECTIONS_SERVICE,
} from '@/modules/admin/modules/collections/services/collections.service';

import { ToastStore } from '@/shared/message/notification';
import { TanstackKey } from '@/shared/types/tanstack';
import type { QueryFunction } from '@tanstack/vue-query';
import { useInfiniteQuery } from '@tanstack/vue-query';
import type { Pagination } from '@/shared/types/Pagination';
import { debounce } from 'lodash';
import type { InsightsProjectAddFormModel } from '../../models/insights-project-add-form.model';
import type { CollectionModel } from '../../../collections/models/collection.model';

const props = defineProps<{
  form: InsightsProjectAddFormModel;
}>();

const cForm = reactive(props.form);

const searchQuery = ref('');
let scrollContainer: HTMLElement | null = null;

const queryKey = computed(() => [
  TanstackKey.ADMIN_COLLECTIONS,
  searchQuery.value,
]);
const queryFn = COLLECTIONS_SERVICE.query(() => ({
  filter: searchQuery.value
    ? {
      name: {
        like: `%${searchQuery.value}%`,
      },
    }
    : {},
  offset: 0,
  limit: 20,
})) as QueryFunction<Pagination<CollectionModel>, readonly unknown[], unknown>;

const {
  data,
  isPending,
  isFetchingNextPage,
  fetchNextPage,
  hasNextPage,
  isSuccess,
  error,
} = useInfiniteQuery<Pagination<CollectionModel>, Error>({
  queryKey,
  queryFn,
  getNextPageParam: (lastPage) => {
    const nextPage = lastPage.offset + lastPage.limit;
    const totalRows = lastPage.total || lastPage.count;
    return nextPage < totalRows ? nextPage : undefined;
  },
  initialPageParam: 0,
});

const collections = computed((): CollectionModel[] => {
  if (isSuccess.value && data.value) {
    const selectedItems = cForm.collections;
    const rows = data.value.pages.reduce(
      (acc, page) => acc.concat(page.rows),
      [] as CollectionModel[],
    );
    return [...selectedItems, ...rows].reduce((acc, item) => {
      if (!acc.find((i) => i.id === item.id)) acc.push(item);
      return acc;
    }, [] as CollectionModel[]);
  }
  return [];
});

watch(error, (err) => {
  if (err) {
    ToastStore.error('Something went wrong while fetching collections');
  }
});

// Debounced search input handler
const debouncedSearch = debounce((query: string) => {
  searchQuery.value = query;
}, 300);

function onSearchInput(query: string) {
  searchQuery.value = query;
  debouncedSearch(query);
}

// Infinite scroll handler
function onScroll(e: Event) {
  if (!scrollContainer) return;
  const threshold = 20;

  const target = e.target as HTMLElement;
  if (
    !isFetchingNextPage.value
    && hasNextPage.value
    && target.scrollHeight - target.scrollTop - target.clientHeight < threshold
  ) {
    fetchNextPage();
  }
}

// Attach scroll listener after dropdown renders
onMounted(() => {
  nextTick(() => {
    scrollContainer = document.querySelector(
      '.collection-infinite-select-dropdown .el-select-dropdown__wrap',
    );
    if (scrollContainer) {
      scrollContainer.addEventListener('scroll', onScroll);
    }
  });
});

onBeforeUnmount(() => {
  if (scrollContainer) {
    scrollContainer.removeEventListener('scroll', onScroll);
  }
});
</script>

<script lang="ts">
export default {
  name: 'LfInsightsProjectsAddCollectionDropdown',
};
</script>

<style>
.collection-infinite-select-dropdown .el-select-dropdown__wrap {
  max-height: 200px;
  overflow: auto;
}

.collection-infinite-select-dropdown .el-select-dropdown__item span {
  max-width: 100%;
  text-overflow: ellipsis;
  overflow: hidden;
}

.el-select-dropdown {
  max-width: 552px;
}
</style>
