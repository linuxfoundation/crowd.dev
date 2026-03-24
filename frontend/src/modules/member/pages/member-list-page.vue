<template>
  <app-page-wrapper size="full-width">
    <div class="member-list-page">
      <div class="mb-10">
        <app-lf-page-header text-class="text-sm text-primary-600 mb-2.5" />
        <div class="flex items-center justify-between">
          <h4>People</h4>
          <div class="flex items-center">
            <router-link
              v-if="membersToMergeCount > 0 && hasPermission(LfPermission.mergeMembers)"
              class="mr-4"
              :to="{
                name: 'memberMergeSuggestions',
                query: { projectGroup: selectedProjectGroup?.id },
              }"
            >
              <lf-button
                type="secondary-gray"
                size="medium"
                class="flex items-center"
              >
                <lf-icon name="shuffle" :size="16" class="text-gray-900 mr-2" />
                <span class="text-gray-900">Merge suggestions</span>
                <span
                  v-if="membersToMergeCount > 0"
                  class="ml-2 bg-primary-100 text-primary-500 py-px px-1.5 leading-5 rounded-full font-semibold"
                >{{ Math.ceil(membersToMergeCount) }}</span>
              </lf-button>
            </router-link>

            <lf-button
              v-if="
                hasPermission(LfPermission.memberCreate)
                  && (hasIntegrations || membersCount > 0)
              "
              type="primary"
              size="medium"
              @click="memberCreate = true"
            >
              Add person
            </lf-button>
          </div>
        </div>
        <div class="text-xs text-gray-500">
          List of all the people who interacted with {{ selectedProjectGroup?.name }} projects
        </div>
      </div>

      <lf-saved-views
        v-model="filters"
        :config="memberSavedViews"
        :filters="memberFilters"
        :custom-filters="customAttributesFilter"
        :static-views="memberStaticViews"
        placement="member"
        @update:model-value="memberFilter.alignFilterList($event)"
      />
      <lf-filter
        v-if="customAttributesFilter"
        ref="memberFilter"
        v-model="filters"
        :config="memberFilters"
        :search-config="memberSearchFilter"
        :saved-views-config="memberSavedViews"
        :custom-config="customAttributesFilter"
        @fetch="fetch($event)"
      />
      <app-member-list-table
        v-model:pagination="pagination"
        :has-integrations="hasIntegrations"
        :has-members="membersCount > 0"
        :is-page-loading="loading"
        :is-table-loading="tableLoading"
        @update:pagination="onPaginationChange"
        @on-add-member="memberCreate = true"
      />
    </div>
  </app-page-wrapper>

  <lf-contributor-add v-if="memberCreate" v-model="memberCreate" />
</template>

<script setup lang="ts">
import LfContributorAdd from '@/modules/contributor/components/edit/contributor-add.vue';
import AppLfPageHeader from '@/modules/lf/layout/components/lf-page-header.vue';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import AppMemberListTable from '@/modules/member/components/list/member-list-table.vue';
import { MemberService } from '@/modules/member/member-service';
import { useMemberStore } from '@/modules/member/store/pinia';
import AppPageWrapper from '@/shared/layout/page-wrapper.vue';
import LfFilter from '@/shared/modules/filters/components/Filter.vue';
import { FilterQuery } from '@/shared/modules/filters/types/FilterQuery';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import LfSavedViews from '@/shared/modules/saved-views/components/SavedViews.vue';
import { TanstackKey } from '@/shared/types/tanstack';
import { mapGetters } from '@/shared/vuex/vuex.helpers';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { useQuery, useQueryClient } from '@tanstack/vue-query';
import { storeToRefs } from 'pinia';
import {
  computed,
  onMounted,
  ref,
  watch,
} from 'vue';
import { filterApiService } from '@/shared/modules/filters/services/filter-api.service';
import { memberFilters, memberSearchFilter } from '../config/filters/main';
import { memberSavedViews, memberStaticViews } from '../config/saved-views/main';

const { buildApiFilter } = filterApiService();

const memberStore = useMemberStore();
const { filters, customAttributesFilter } = storeToRefs(memberStore);

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const memberCreate = ref(false);

const { listByPlatform } = mapGetters('integration');

const { hasPermission } = usePermissions();

const memberFilter = ref<InstanceType<typeof LfFilter> | null>(null);

const hasIntegrations = computed(() => !!Object.keys(listByPlatform.value || {}).length);

const queryClient = useQueryClient();

const pagination = ref({
  page: 1,
  perPage: 20,
});

// Reactive state for query parameters
const queryParams = ref({
  search: '',
  filter: filters.value,
  offset: 0,
  limit: 20,
  orderBy: 'activityCount_DESC',
  segments: selectedProjectGroup.value?.id ? [selectedProjectGroup.value.id] : [],
});

// Create a computed query key for members
const membersQueryKey = computed(() => [
  TanstackKey.MEMBERS_LIST,
  selectedProjectGroup.value?.id,
  queryParams.value.search,
  filters.value, // Use filters.value directly to make it reactive
  queryParams.value.offset,
  queryParams.value.limit,
  queryParams.value.orderBy,
  queryParams.value.segments,
]);

// Query for members list with caching
const {
  data: membersData,
  isLoading: membersLoading,
  isFetching: membersFetching,
} = useQuery({
  queryKey: membersQueryKey,
  queryFn: () => {
    const transformedFilter = filters.value
      ? buildApiFilter(
        filters.value,
        { ...memberFilters, ...customAttributesFilter.value },
        memberSearchFilter,
        memberSavedViews,
      )
      : { search: '', filter: {}, orderBy: 'activityCount_DESC' };

    return MemberService.listMembers({
      search: queryParams.value.search,
      filter: transformedFilter.filter,
      offset: queryParams.value.offset,
      limit: queryParams.value.limit,
      orderBy: transformedFilter.orderBy,
      segments: queryParams.value.segments,
    });
  },
  enabled: !!selectedProjectGroup.value?.id,
});

// Create a computed query key for merge suggestions
const mergeSuggestionsQueryKey = computed(() => [
  TanstackKey.MEMBER_MERGE_SUGGESTIONS_COUNT,
  selectedProjectGroup.value?.id,
]);

// Query for merge suggestions count with caching
const {
  data: mergeSuggestionsData,
} = useQuery({
  queryKey: mergeSuggestionsQueryKey,
  queryFn: () => MemberService.fetchMergeSuggestions(0, 0, { countOnly: true }),
  enabled: !!selectedProjectGroup.value?.id,
});

// Watch for members data changes and update the store
watch(membersData, (newData) => {
  if (newData) {
    // Update the Pinia store with the new data
    memberStore.members = newData.rows || [];
    memberStore.totalMembers = newData.count || 0;

    // Build the correct transformed filter for savedFilterBody
    const transformedFilter = filters.value
      ? buildApiFilter(
        filters.value,
        { ...memberFilters, ...customAttributesFilter.value },
        memberSearchFilter,
        memberSavedViews,
      )
      : { search: '', filter: {}, orderBy: 'activityCount_DESC' };

    memberStore.savedFilterBody = {
      search: queryParams.value.search,
      filter: transformedFilter.filter,
      offset: queryParams.value.offset,
      limit: queryParams.value.limit,
      orderBy: transformedFilter.orderBy,
    };
  }
}, { immediate: true });

// Computed properties derived from queries
const membersCount = computed(() => membersData.value?.count || 0);
const membersToMergeCount = computed(() => mergeSuggestionsData.value?.count || 0);
const loading = computed(() => membersLoading.value);
const tableLoading = computed(() => membersFetching.value);

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

// Watch for filter changes to ensure cache invalidation
watch(
  filters,
  () => {
    // Reset to first page when filters change
    pagination.value.page = 1;
    queryParams.value.offset = 0;
  },
  { deep: true },
);

watch(
  selectedProjectGroup,
  (newProjectGroup, oldProjectGroup) => {
    if (newProjectGroup?.id !== oldProjectGroup?.id) {
      pagination.value.page = 1;

      // Reset query params for new project group
      queryParams.value = {
        search: '',
        filter: filters.value,
        offset: 0,
        limit: pagination.value.perPage,
        orderBy: 'activityCount_DESC',
        segments: newProjectGroup ? [newProjectGroup?.id] : [],
      };

      // Invalidate all related caches
      queryClient.invalidateQueries({
        queryKey: [TanstackKey.MEMBERS_LIST],
      });
      queryClient.invalidateQueries({
        queryKey: [TanstackKey.MEMBER_MERGE_SUGGESTIONS_COUNT],
      });
    }
  },
);

onMounted(() => {
  memberStore.getMemberCustomAttributes();
  (window as any).analytics.page('Members');
});
</script>
