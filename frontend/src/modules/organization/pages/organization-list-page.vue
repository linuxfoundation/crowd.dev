<template>
  <app-page-wrapper size="full-width">
    <div class="member-list-page">
      <div class="mb-10">
        <app-lf-page-header text-class="text-sm text-primary-600 mb-2.5" />
        <div class="flex items-center justify-between">
          <div class="flex items-center">
            <h4>Organizations</h4>
          </div>
          <div class="flex items-center">
            <el-tooltip
              v-if="organizationsToMergeCount > 0"
              content="Coming soon"
              placement="top"
              :disabled="hasPermission(LfPermission.mergeOrganizations)"
            >
              <span>
                <component
                  :is="hasPermission(LfPermission.mergeOrganizations) ? 'router-link' : 'span'"
                  class=" mr-4 "
                  :to="{
                    name: 'organizationMergeSuggestions',
                    query: {
                      projectGroup: selectedProjectGroup?.id,
                    },
                  }"
                >
                  <lf-button
                    :disabled="!hasPermission(LfPermission.mergeOrganizations)"
                    type="secondary-gray"
                    size="medium"
                    class="flex items-center"
                  >
                    <lf-icon name="shuffle" :size="16" class="mr-2 text-gray-900" />
                    <span class="text-gray-900">Merge suggestions</span>
                    <span
                      v-if="organizationsToMergeCount > 0"
                      class="ml-2 bg-primary-100 text-primary-500 py-px px-1.5 leading-5 rounded-full font-semibold"
                    >{{ Math.ceil(organizationsToMergeCount) }}</span>
                  </lf-button>
                </component>
              </span>
            </el-tooltip>
            <lf-button
              v-if="hasPermission(LfPermission.organizationCreate)"
              type="primary"
              size="medium"
              @click="isOrganizationCreateModalOpen = true"
            >
              Add organization
            </lf-button>
          </div>
        </div>
        <div class="text-xs text-gray-500">
          List of all the organizations that relate to {{ selectedProjectGroup?.name }} projects
        </div>
      </div>

      <lf-saved-views
        v-model="filters"
        :config="organizationSavedViews"
        :filters="organizationFilters"
        placement="organization"
        @update:model-value="organizationFilter.alignFilterList($event)"
      />
      <lf-filter
        ref="organizationFilter"
        v-model="filters"
        :config="organizationFilters"
        :search-config="organizationSearchFilter"
        :saved-views-config="organizationSavedViews"
        @fetch="fetch($event)"
      />
      <app-organization-list-table
        v-model:pagination="pagination"
        :has-organizations="organizationStore.totalOrganizations > 0"
        :is-page-loading="loading"
        :is-table-loading="tableLoading"
        @update:pagination="onPaginationChange"
        @on-add-organization="isOrganizationCreateModalOpen = true"
      />
    </div>
  </app-page-wrapper>

  <lf-organization-add v-if="isOrganizationCreateModalOpen" v-model="isOrganizationCreateModalOpen" />
</template>

<script setup lang="ts">
import AppLfPageHeader from '@/modules/lf/layout/components/lf-page-header.vue';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import LfOrganizationAdd from '@/modules/organization/components/edit/organization-add.vue';
import AppOrganizationListTable from '@/modules/organization/components/list/organization-list-table.vue';
import { organizationFilters, organizationSearchFilter } from '@/modules/organization/config/filters/main';
import { organizationSavedViews } from '@/modules/organization/config/saved-views/main';
import { OrganizationService } from '@/modules/organization/organization-service';
import { useOrganizationStore } from '@/modules/organization/store/pinia';
import AppPageWrapper from '@/shared/layout/page-wrapper.vue';
import LfFilter from '@/shared/modules/filters/components/Filter.vue';
import { filterApiService } from '@/shared/modules/filters/services/filter-api.service';
import { FilterQuery } from '@/shared/modules/filters/types/FilterQuery';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import LfSavedViews from '@/shared/modules/saved-views/components/SavedViews.vue';
import { TanstackKey } from '@/shared/types/tanstack';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { useQuery } from '@tanstack/vue-query';
import { storeToRefs } from 'pinia';
import {
  computed, onMounted, ref, watch,
} from 'vue';

const { buildApiFilter } = filterApiService();

const organizationStore = useOrganizationStore();
const { filters } = storeToRefs(organizationStore);

const isOrganizationCreateModalOpen = ref(false);

const organizationFilter = ref<InstanceType<typeof LfFilter> | null>(null);

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const { hasPermission } = usePermissions();

const pagination = ref({
  page: 1,
  perPage: 20,
});

// Reactive state for query parameters
const { search: initialSearch, filter: initialFilter, orderBy: initialOrderBy } = buildApiFilter(
  filters.value,
  organizationFilters,
  organizationSearchFilter,
  organizationSavedViews,
);

const queryParams = ref({
  search: initialSearch || '',
  filter: initialFilter,
  offset: 0,
  limit: 20,
  orderBy: initialOrderBy || 'activityCount_DESC',
  segments: selectedProjectGroup.value?.id ? [selectedProjectGroup.value.id] : [],
});

// Create a computed query key for organizations
const organizationsQueryKey = computed(() => [
  TanstackKey.ORGANIZATIONS_LIST,
  selectedProjectGroup.value?.id,
  queryParams.value.search,
  queryParams.value.filter,
  queryParams.value.offset,
  queryParams.value.limit,
  queryParams.value.orderBy,
  queryParams.value.segments,
]);

// Query for organizations list with caching
const {
  data: organizationsData,
  isLoading: organizationsLoading,
  isFetching: organizationsFetching,
} = useQuery({
  queryKey: organizationsQueryKey,
  queryFn: () => OrganizationService.query({
    search: queryParams.value.search,
    filter: queryParams.value.filter,
    offset: queryParams.value.offset,
    limit: queryParams.value.limit,
    orderBy: queryParams.value.orderBy,
    segments: queryParams.value.segments,
  }),
  enabled: !!selectedProjectGroup.value?.id,
});

// Create a computed query key for merge suggestions
const mergeSuggestionsQueryKey = computed(() => [
  TanstackKey.ORGANIZATION_MERGE_SUGGESTIONS_COUNT,
  selectedProjectGroup.value?.id,
]);

// Query for merge suggestions count with caching
const {
  data: mergeSuggestionsData,
} = useQuery({
  queryKey: mergeSuggestionsQueryKey,
  queryFn: () => OrganizationService.fetchMergeSuggestions(0, 0, { countOnly: true }),
  enabled: !!selectedProjectGroup.value?.id,
});

// Watch for organizations data changes and update the store
watch(organizationsData, (newData) => {
  if (newData) {
    organizationStore.organizations = newData.rows || [];
    organizationStore.totalOrganizations = newData.count || 0;
    organizationStore.savedFilterBody = {
      search: queryParams.value.search,
      filter: queryParams.value.filter,
      offset: queryParams.value.offset,
      limit: queryParams.value.limit,
      orderBy: queryParams.value.orderBy,
    };
  }
}, { immediate: true });

// Computed properties derived from queries
const organizationsToMergeCount = computed(() => mergeSuggestionsData.value?.count || 0);
const loading = computed(() => organizationsLoading.value);
const tableLoading = computed(() => organizationsFetching.value);

const fetch = ({
  search, filter, orderBy, body,
}: FilterQuery) => {
  // Update query parameters
  queryParams.value = {
    search: search || '',
    filter: filter || {},
    offset: 0,
    limit: pagination.value.perPage,
    orderBy: orderBy || 'activityCount_DESC',
    segments: selectedProjectGroup.value?.id ? [selectedProjectGroup.value.id] : [],
    ...body,
  };

  pagination.value.page = 1;
};

const onPaginationChange = ({
  page, perPage,
}: { page: number; perPage: number }) => {
  // Update only pagination parameters
  queryParams.value = {
    ...queryParams.value,
    offset: (page - 1) * perPage || 0,
    limit: perPage || 20,
  };

  pagination.value.page = page;
  pagination.value.perPage = perPage;
};

watch(
  selectedProjectGroup,
  (newProjectGroup, oldProjectGroup) => {
    if (newProjectGroup?.id !== oldProjectGroup?.id) {
      pagination.value.page = 1;

      const { filter: resetFilter, orderBy: resetOrderBy } = buildApiFilter(
        filters.value,
        organizationFilters,
        organizationSearchFilter,
        organizationSavedViews,
      );

      // Reset query params for new project group
      queryParams.value = {
        search: '',
        filter: resetFilter,
        offset: 0,
        limit: pagination.value.perPage,
        orderBy: resetOrderBy || 'activityCount_DESC',
        segments: newProjectGroup ? [newProjectGroup?.id] : [],
      };
    }
  },
);

onMounted(() => {
  (window as any).analytics.page('Organization');
});
</script>
