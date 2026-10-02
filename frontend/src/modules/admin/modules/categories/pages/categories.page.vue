<template>
  <header class="py-6">
    <div class="flex items-center justify-between gap-4">
      <div>
        <lf-category-group-type-filter v-model="type" />
      </div>
      <div class="flex-grow">
        <lf-search
          v-model="search"
          lazy
          placeholder="Search category groups, categories..."
          class="!h-9"
        />
      </div>
      <lf-button type="secondary-ghost" @click="addCategoryGroup()">
        <lf-icon name="plus" />
        Add category group
      </lf-button>
    </div>
  </header>
  <div v-if="categoryGroups.length > 0">
    <lf-table>
      <thead>
        <tr>
          <th>Category group</th>
          <th>Categories</th>
          <th>Type</th>
          <th />
        </tr>
      </thead>
      <tbody>
        <tr v-for="group of categoryGroups" :key="group.id">
          <td>
            <p class="text-medium font-semibold">
              {{ group.name }}
            </p>
          </td>
          <td>
            <lf-popover placement="top" trigger-event="hover">
              <template #trigger>
                <lf-badge type="secondary" class="!rounded-full !px-2.5">
                  {{ pluralize('category', group.categories?.length || 0, true) }}
                </lf-badge>
              </template>
              <div>
                <p class="text-small font-semibold text-gray-400 pb-3">
                  Categories
                </p>
                <div class="flex flex-wrap gap-2">
                  <lf-badge v-for="category in group.categories" :key="category.id" type="secondary">
                    {{ category.name }}
                  </lf-badge>
                </div>
              </div>
            </lf-popover>
          </td>
          <td>
            <p class="text-medium">
              {{ group.type === CategoryGroupType.VERTICAL ? 'Industry' : 'Stack' }}
            </p>
          </td>
          <td class="w-10">
            <div class="flex justify-end">
              <lf-category-group-dropdown
                :category-group="group"
                @edit="editCategoryGroup(group)"
                @reload="reload()"
              >
                <lf-button type="secondary-ghost" icon-only>
                  <lf-icon name="ellipsis" />
                </lf-button>
              </lf-category-group-dropdown>
            </div>
          </td>
        </tr>
      </tbody>
    </lf-table>
  </div>
  <div v-else-if="!loading" class="pt-14 flex flex-col items-center">
    <lf-icon name="eyes" :size="64" class="text-gray-300" />
    <h6 class="text-center pt-6 font-semibold text-gray-900">
      No category groups found
    </h6>
    <p class="text-center pt-3 text-gray-500 text-small">
      We couldn't find any results that match your search or filters criteria, please try a different query.
    </p>
  </div>
  <div class="pt-4 flex justify-center items-center">
    <p v-if="loading" class="text-small text-gray-400">
      {{ pageSize }} out of {{ total }} collections
    </p>
    <lf-button
      v-if="categoryGroups.length < total"
      type="primary-ghost"
      loading-text="Loading categories..."
      :loading="loading"
      @click="loadMore()"
    >
      Load more
    </lf-button>
  </div>
  <lf-category-group-form
    v-if="isCreateGroupOpen"
    v-model="isCreateGroupOpen"
    :category-group="selectedCategoryGroup || undefined"
    @update:model-value="reload()"
  />
</template>

<script setup lang="ts">
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfCategoryGroupForm from '@/modules/admin/modules/categories/components/form/category-group-form.vue';
import { onMounted, ref, watch } from 'vue';
import { CategoryGroupService } from '@/modules/admin/modules/categories/services/category-group.service';
import type { CategoryGroup } from '@/modules/admin/modules/categories/types/CategoryGroup';
import { CategoryGroupType } from '@/modules/admin/modules/categories/types/CategoryGroup';
import LfTable from '@/ui-kit/table/Table.vue';
import LfBadge from '@/ui-kit/badge/Badge.vue';
import LfCategoryGroupDropdown from '@/modules/admin/modules/categories/components/list/category-group-dropdown.vue';
import LfSearch from '@/ui-kit/search/Search.vue';
import LfCategoryGroupTypeFilter
  from '@/modules/admin/modules/categories/components/list/category-group-type-filter.vue';
import { ToastStore } from '@/shared/message/notification';
import pluralize from 'pluralize';
import LfPopover from '@/ui-kit/popover/Popover.vue';

const categoryGroups = ref<CategoryGroup[]>([]);
const isCreateGroupOpen = ref<boolean>(false);
const selectedCategoryGroup = ref<null | CategoryGroup>(null);

const search = ref<string>('');
const type = ref<string>('');
const page = ref<number>(0);
const pageSize = ref<number>(20);
const total = ref<number>(0);

const loading = ref<boolean>(true);

const fetchCategoryGroups = () => {
  loading.value = true;
  CategoryGroupService.list({
    offset: page.value * pageSize.value,
    limit: pageSize.value,
    query: search.value,
    type: type.value,
  })
    .then((res) => {
      if (res.offset > 0) {
        categoryGroups.value = [...categoryGroups.value, ...res.rows];
      } else {
        categoryGroups.value = res.rows;
      }
      total.value = res.count;
    })
    .catch(() => {
      ToastStore.error('Error loading category groups');
    })
    .finally(() => {
      loading.value = false;
    });
};

const reload = () => {
  page.value = 0;
  fetchCategoryGroups();
};

const loadMore = () => {
  page.value += 1;
  fetchCategoryGroups();
};

const addCategoryGroup = () => {
  selectedCategoryGroup.value = null;
  isCreateGroupOpen.value = true;
};

const editCategoryGroup = (categoryGroup: CategoryGroup) => {
  selectedCategoryGroup.value = categoryGroup;
  isCreateGroupOpen.value = true;
};

watch([type, search], () => {
  page.value = 0;
  fetchCategoryGroups();
});

onMounted(() => {
  fetchCategoryGroups();
});
</script>

<script lang="ts">
export default {
  name: 'LfCategoriesPage',
};
</script>
