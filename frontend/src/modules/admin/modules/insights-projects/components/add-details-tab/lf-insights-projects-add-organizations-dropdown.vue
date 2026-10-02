<template>
  <el-select
    v-model="cForm.organizationId"
    filterable
    :filter-method="onSearchInput"
    placeholder="Select company"
    class="w-full"
    popper-class="organization-infinite-select-dropdown"
    @change="onOrganizationChange"
  >
    <template v-if="cForm.organizationId" #prefix>
      <lf-avatar
        :src="cForm.organization.logo"
        :name="cForm.organization.displayName"
        :size="24"
        class="!rounded-md border border-gray-200"
      />
    </template>
    <template #label="{ label, value }">
      <span>{{ label }}: </span>
      <span style="font-weight: bold">{{ value }}</span>
    </template>
    <el-option
      v-for="item in organizations"
      :key="item.id"
      :label="item.displayName"
      :value="item.id"
    >
      <lf-avatar
        :src="item.logo"
        :name="item.displayName"
        :size="24"
        class="!rounded-md border border-gray-200"
      />
      <span class="ml-2 text-gray-900 text-sm">{{ item.displayName }}</span>
    </el-option>
    <el-option
      v-if="loading && organizations.length > 0"
      :key="'loading'"
      label="Loading..."
      value=""
      disabled
    />
  </el-select>
</template>

<script setup lang="ts">
import { debounce } from 'lodash';
import {
  nextTick, onBeforeUnmount, onMounted, reactive, ref,
} from 'vue';

import { ToastStore } from '@/shared/message/notification';
import { useOrganizationStore } from '@/modules/organization/store/pinia';
import type { Organization } from '@/modules/organization/types/Organization';
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import type { InsightsProjectAddFormModel } from '../../models/insights-project-add-form.model';

const props = defineProps<{
  form: InsightsProjectAddFormModel;
}>();
const cForm = reactive(props.form);

const loading = ref(false);
const organizations = ref<Organization[]>([]);
const page = ref(0);
const pageSize = 20;
const noMoreData = ref(false);
const searchQuery = ref('');
let scrollContainer: HTMLElement | null = null;

const organizationStore = useOrganizationStore();

// Your API service method
function fetchOrganizations(query = '', pageNum = 0) {
  loading.value = true;
  organizationStore
    .fetchOrganizations({
      body: {
        filter: query
          ? {
            displayName: {
              textContains: query,
            },
          }
          : {},
        offset: pageNum * pageSize,
        limit: pageSize,
        orderBy: 'activityCount_DESC',
        segments: [cForm.segmentId],
      },
    })
    .then((res) => {
      const { rows } = res;
      const selectedItem = cForm.organizationId ? [cForm.organization as Organization] : [];
      if (pageNum === 0) {
        organizations.value = [...selectedItem, ...rows].reduce((acc, item) => {
          if (!acc.find((i) => i.id === item.id)) acc.push(item);
          return acc;
        }, [] as Organization[]);
      } else {
        organizations.value = [...organizations.value, ...rows].reduce((acc, item) => {
          if (!acc.find((i) => i.id === item.id)) acc.push(item);
          return acc;
        }, [] as Organization[]);
      }

      noMoreData.value = organizations.value.length >= +res.count;
    })
    .catch(() => {
      ToastStore.closeAll();
      ToastStore.error('Failed to load  organizations');
    })
    .finally(() => {
      loading.value = false;
    });
}

// Debounced search input handler
const debouncedSearch = debounce((query: string) => {
  page.value = 0;
  noMoreData.value = false;
  fetchOrganizations(query, page.value);
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
    !loading.value
    && !noMoreData.value
    && target.scrollHeight - target.scrollTop - target.clientHeight < threshold
  ) {
    page.value += 1;
    fetchOrganizations(searchQuery.value, page.value);
  }
}

const onOrganizationChange = (value: string) => {
  const organization = organizations.value.find(
    (organization) => organization.id === value,
  );
  if (organization) {
    Object.assign(cForm.organization, organization);
  }
};

// Attach scroll listener after dropdown renders
onMounted(() => {
  fetchOrganizations('', 0);
  nextTick(() => {
    scrollContainer = document.querySelector(
      '.organization-infinite-select-dropdown .el-select-dropdown__wrap',
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
.organization-infinite-select-dropdown .el-select-dropdown__wrap {
  max-height: 200px;
  overflow: auto;
}
</style>
