<template>
  <div>
    <div
      v-if="collections.length > 0 || search.length > 0"
      class="flex gap-4 pt-6 pb-4"
    >
      <!-- Search input -->
      <lf-search
        v-model="search"
        class="h-9 flex-grow"
        :lazy="true"
        placeholder="Search collections..."
      />
      <lf-button
        size="medium"
        type="secondary-ghost"
        @click="openCollectionAdd"
      >
        <lf-icon name="rectangle-history-circle-plus" :size="16" />
        Add collection
      </lf-button>
    </div>
    <div v-if="collections.length > 0">
      <lf-collection-table
        :collections="collections"
        @on-edit-collection="onEditCollection($event)"
        @on-star-collection="ontoggleStar($event)"
        @on-delete-collection="openRemoveCollectionDialog($event)"
      />
      <div class="pt-4">
        <lf-button
          v-if="hasNextPage && !isFetchingNextPage"
          type="primary-ghost"
          @click="loadMore()"
        >
          Load more
        </lf-button>
        <div
          v-else-if="isFetchingNextPage"
          class="flex items-center justify-center"
        >
          <span class="text-xs text-gray-400 mr-4">
            {{ collections.length }} out of {{ total }} collections
          </span>
          <div class="flex items-center text-xs text-primary-200">
            <lf-spinner :size="'1rem'" class="mr-1 border-primary-200" />
            Loading collections...
          </div>
        </div>
      </div>
    </div>

    <div v-else-if="!isPending" class="flex flex-col items-center">
      <app-empty-state-cta
        v-if="search.length"
        class="w-full !pb-0"
        icon="rectangle-history"
        :icon-size="100"
        title="No collections found"
        description="We couldn't find any results that match your search criteria, please try a different query"
      />
      <template v-else>
        <app-empty-state-cta
          class="w-full !pb-0"
          icon="rectangle-history"
          :icon-size="100"
          title="No collections yet"
          description="Start creating project collections of a certain area or tech stack"
        />
        <lf-button
          class="w-fit"
          size="medium"
          type="primary-ghost"
          @click="openCollectionAdd"
        >
          <lf-icon name="rectangle-history-circle-plus" :size="16" />
          Add collection
        </lf-button>
      </template>
    </div>
    <div v-else class="pt-8 flex justify-center">
      <lf-spinner />
    </div>
  </div>

  <lf-collection-add
    v-if="isCollectionDialogOpen"
    v-model="isCollectionDialogOpen"
    :collection="collectionEditObject"
    @on-collection-created="onCollectionDialogCloseSuccess"
    @on-collection-edited="onCollectionDialogCloseSuccess"
    @update:model-value="onCollectionDialogClose"
  />

  <app-delete-confirm-dialog
    v-if="removeCollection"
    v-model="removeCollection"
    title="Are you sure you want to delete this collection?"
    description="This will delete the collection permanently. You can’t undo this action."
    icon="trash-can"
    confirm-button-text="Delete collection"
    cancel-button-text="Cancel"
    confirm-text="delete"
    @confirm="deleteCollectionMutation.mutate(removeCollectionId)"
    @close="closeRemoveDialog"
  />
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import LfSearch from '@/ui-kit/search/Search.vue';
import {
  COLLECTIONS_SERVICE,
} from '@/modules/admin/modules/collections/services/collections.service';
import type { CollectionModel, CollectionRequest } from '@/modules/admin/modules/collections/models/collection.model';
import LfCollectionAdd from '@/modules/admin/modules/collections/components/lf-collection-add.vue';

import { ToastStore } from '@/shared/message/notification';
import LfCollectionTable from '@/modules/admin/modules/collections/components/lf-collection-table.vue';
import AppEmptyStateCta from '@/shared/empty-state/empty-state-cta.vue';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import AppDeleteConfirmDialog from '@/shared/dialog/delete-confirm-dialog.vue';
import { cloneDeep } from 'lodash';
import type {
  QueryFunction,
} from '@tanstack/vue-query';
import { useInfiniteQuery, useMutation, useQueryClient } from '@tanstack/vue-query';
import { TanstackKey } from '@/shared/types/tanstack';
import { useDebounce } from '@vueuse/core';
import type { Pagination } from '@/shared/types/Pagination';

const queryClient = useQueryClient();

const search = ref('');
const searchValue = useDebounce(search, 300);

const removeCollection = ref<boolean>(false);
const removeCollectionId = ref<string>('');
const isCollectionDialogOpen = ref<boolean>(false);
const collectionEditObject = ref<CollectionModel | undefined>(undefined);

const queryKey = computed(() => [
  TanstackKey.ADMIN_COLLECTIONS,
  searchValue.value,
]);
const queryFn = COLLECTIONS_SERVICE.query(() => ({
  filter: searchValue.value
    ? {
      name: {
        like: `%${searchValue.value}%`,
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
    return data.value.pages.reduce(
      (acc, page) => acc.concat(page.rows),
        [] as CollectionModel[],
    );
  }
  return [];
});

const total = computed((): number => {
  if (isSuccess.value && data.value) {
    return data.value.pages[0].total || data.value.pages[0].count;
  }
  return 0;
});

watch(error, (err) => {
  if (err) {
    ToastStore.error('Something went wrong while fetching collections');
  }
});

const loadMore = () => {
  if (hasNextPage.value && !isFetchingNextPage.value) {
    fetchNextPage();
  }
};

const openCollectionAdd = () => {
  isCollectionDialogOpen.value = true;
};

const onEditCollection = (collectionId: string) => {
  isCollectionDialogOpen.value = true;
  collectionEditObject.value = cloneDeep(
    collections.value.find((collection) => collection.id === collectionId),
  );
};

const updateMutation = useMutation({
  mutationFn: ({ id, collection }: { id: string; collection: CollectionRequest }) => COLLECTIONS_SERVICE.update(id, collection),
  onMutate: () => {
    ToastStore.info('Collection is being featured');
  },
  onSuccess: (collection: CollectionRequest) => {
    ToastStore.closeAll();
    ToastStore.success(`Collection successfully ${collection!.starred ? 'mark as featured' : 'unmark as featured'}`);
    queryClient.invalidateQueries({
      queryKey: [TanstackKey.ADMIN_COLLECTIONS],
    });
  },
  onError: () => {
    ToastStore.closeAll();
    ToastStore.error('Something went wrong');
  },
});

const ontoggleStar = (collectionId: string) => {
  const collection = collections.value.find((collection) => collection.id === collectionId);
  if (!collection) {
    ToastStore.closeAll();
    ToastStore.error('Collection not found');
    return;
  }
  updateMutation.mutate({
    id: collectionId,
    collection: {
      categoryId: collection.categoryId || null,
      description: collection.description,
      name: collection.name,
      projects: collection.projects.map((project) => ({
        id: project.id,
        starred: project.starred,
      })),
      slug: collection.slug,
      starred: !collection.starred,
    },
  });
};

const onCollectionDialogCloseSuccess = () => {
  isCollectionDialogOpen.value = false;
  collectionEditObject.value = undefined;
};

const onCollectionDialogClose = () => {
  isCollectionDialogOpen.value = false;
  collectionEditObject.value = undefined;
};

const openRemoveCollectionDialog = (collectionId: string) => {
  removeCollectionId.value = collectionId;
  removeCollection.value = true;
};

const deleteCollectionMutation = useMutation({
  mutationFn: (collectionId: string) => COLLECTIONS_SERVICE.delete(collectionId),
  onSuccess: () => {
    ToastStore.closeAll();
    ToastStore.success('Collection successfully deleted');
    queryClient.invalidateQueries({
      queryKey: [TanstackKey.ADMIN_COLLECTIONS],
    });
    closeRemoveDialog();
  },
  onError: () => {
    ToastStore.closeAll();
    ToastStore.error('Something went wrong');
    closeRemoveDialog();
  },
  onMutate: () => {
    ToastStore.info('Collection is being deleted');
  },
});

const closeRemoveDialog = () => {
  removeCollection.value = false;
  removeCollectionId.value = '';
};
</script>

<script lang="ts">
export default {
  name: 'LfCollectionsPage',
};
</script>
