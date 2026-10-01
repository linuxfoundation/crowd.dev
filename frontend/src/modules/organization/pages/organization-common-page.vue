<template>
  <div class="pt-8">
    <lf-filter
      ref="organizationFilter"
      v-model="filters"
      :config="organizationCommonFilters"
      :search-config="organizationSearchFilter"
      :saved-views-config="commonOrganizationSavedViews"
      hash="organizations"
      class="pb-1"
      @fetch="fetch($event)"
    />
    <div
      v-if="loading"
      class="h-16 !relative !min-h-5 flex justify-center items-center"
    >
      <div class="animate-spin w-fit">
        <div class="custom-spinner" />
      </div>
    </div>
    <div v-else>
      <div class="pb-4 text-small text-gray-400 -mt-4">
        {{ pluralize('organization', totalOrganizations, true) }}
      </div>
      <app-organization-common-list-table
        v-model:sorting="sorting"
        :organizations="organizations"
        @update:sorting="getOrganizations"
        @reload="getOrganizations()"
      >
        <template #pagination>
          <app-pagination
            :total="totalOrganizations"
            :page-size="Number(pagination.perPage)"
            :current-page="pagination.page || 1"
            :hide-sorting="true"
            @change-current-page="onPaginationChange({ page: $event })"
            @change-page-size="onPaginationChange({ perPage: $event })"
          />
        </template>
      </app-organization-common-list-table>
    </div>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue';
import LfFilter from '@/shared/modules/filters/components/Filter.vue';
import {
  organizationCommonFilters,
  organizationSearchFilter,
} from '@/modules/organization/config/filters/main';
import { commonOrganizationSavedViews } from '@/modules/organization/config/saved-views/main';
import type { FilterQuery } from '@/shared/modules/filters/types/FilterQuery';
import AppOrganizationCommonListTable from '@/modules/organization/components/list/organization-common-list-table.vue';
import { OrganizationService } from '@/modules/organization/organization-service';
import type { Pagination } from '@/shared/types/Pagination';
import type { Organization } from '@/modules/organization/types/Organization';
import AppPagination from '@/shared/pagination/pagination.vue';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import pluralize from 'pluralize';

const { listProjectGroups } = useLfSegmentsStore();

const loading = ref(true);

const filters = ref({
  ...commonOrganizationSavedViews.defaultView.config,
});

const savedBody = ref({});

const pagination = ref({
  page: 1,
  perPage: 20,
  total: 0,
});

const sorting = ref('activityCount_DESC');

const organizations = ref<any[]>([]);
const totalOrganizations = ref<number>(0);

const getOrganizations = (body?: any) => {
  savedBody.value = {
    ...savedBody.value,
    ...body,
  };
  loading.value = true;
  OrganizationService.organizationsList({
    ...savedBody.value,
    orderBy: sorting.value,
    excludeSegments: true,
  })
    .then((data: Pagination<Organization>) => {
      organizations.value = data.rows;
      totalOrganizations.value = data.count;
    })
    .catch((err: Error) => {
      organizations.value = [];
      totalOrganizations.value = 0;
    })
    .finally(() => {
      loading.value = false;
    });
};

const fetch = ({
  filter, body,
}: FilterQuery) => {
  getOrganizations({
    ...body,
    filter,
    offset: 0,
    limit: pagination.value.perPage,
  });
};

const onPaginationChange = (paginationData: any) => {
  pagination.value = {
    ...pagination.value,
    ...paginationData,
  };

  getOrganizations({
    offset: (pagination.value.page - 1) * pagination.value.perPage,
    limit: pagination.value.perPage,
  });
};

onMounted(() => {
  listProjectGroups({
    limit: null,
    reset: true,
  } as any);
});
</script>

<script lang="ts">
export default {
  name: 'AppOrganizationCommonPage',
};
</script>
