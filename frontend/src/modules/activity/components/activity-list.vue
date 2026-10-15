<template>
  <div class="pt-3">
    <lf-filter
      v-model="filters"
      :config="activityFilters"
      :search-config="activitySearchFilter"
      hash="activity"
      @fetch="fetch($event)"
    />
    <div
      v-if="loading && !activities.length"
      class="flex flex-col items-center mt-10"
    >
      <div
        class="h-16 !relative !min-h-5 flex justify-center items-center"
      >
        <div class="animate-spin w-fit">
          <div class="custom-spinner" />
        </div>
      </div>
      <div class="text-gray-500 italic text-xs">
        This might take up to 10 seconds for a large project group
      </div>
    </div>
    <div v-else>
      <!-- Empty State -->
      <app-empty-state-cta
        v-if="activities.length === 0"
        icon="list"
        :title="emptyState.title"
        :description="emptyState.description"
      />

      <div v-else>
        <!-- Sorter -->
        <div class="mb-4">
          <!-- <app-pagination-sorter
            v-model="sorterFilter"
            :page-size="Number(pagination.perPage)"
            :total="totalActivities"
            :current-page="pagination.page"
            :has-page-counter="false"
            :sorter="false"
            module="activity"
            position="top"
          /> -->
        </div>

        <!-- Activity item list -->
        <app-activity-item
          v-for="activity of activities"
          :key="activity.id"
          :activity="activity"
          class="mb-6"
          v-bind="cardOptions"
        />

        <!-- Load more button -->
        <app-load-more
          :is-visible="isLoadMoreVisible"
          :is-loading="loading"
          :fetch-fn="onLoadMore"
        />
      </div>
    </div>
  </div>
</template>

<script setup>
import {
  computed,
  ref,
} from 'vue';
import AppActivityItem from '@/modules/activity/components/activity-item.vue';
// import AppPaginationSorter from '@/shared/pagination/pagination-sorter.vue';
import AppEmptyStateCta from '@/shared/empty-state/empty-state-cta.vue';
import LfFilter from '@/shared/modules/filters/components/Filter.vue';
import { useActivityStore } from '@/modules/activity/store/pinia';
import { storeToRefs } from 'pinia';
import { activityFilters, activitySearchFilter } from '@/modules/activity/config/filters/main';
import AppLoadMore from '@/shared/button/load-more.vue';
import { dateHelper } from '@/shared/date-helper/date-helper';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { mergeActivityPages, useActivitiesQuery } from '@/modules/activity/queries/useActivitiesQuery';

// const sorterFilter = ref('trending');

defineProps({
  cardOptions: {
    type: Object,
    required: false,
    default: () => ({}),
  },
});

const activityStore = useActivityStore();
const { filters, timestamp } = storeToRefs(activityStore);
const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());

const params = ref(null);
const {
  data, isFetching, isError, fetchNextPage,
} = useActivitiesQuery(params);

const loading = computed(() => !params.value || isFetching.value);
const activities = computed(() => (isError.value || !data.value ? [] : mergeActivityPages(data.value.pages)));
const totalActivities = computed(() => (isError.value || !data.value ? 0 : data.value.pages.at(-1).count));

filters.value = {
  search: '',
  relation: 'and',
  order: {
    prop: 'timestamp',
    order: 'descending',
  },
  date: {
    include: true,
    value: dateHelper().utc().subtract(6, 'day').format('YYYY-MM-DD'),
  },
};

const emptyState = computed(() => ({
  title: 'No activities found',
  description:
        "We couldn't find any results that match your search criteria, please try a different query",
}));

const isLoadMoreVisible = computed(() => activities.value.length < totalActivities.value);

const onLoadMore = () => {
  timestamp.value = activities.value.at(-1).timestamp;
  fetchNextPage();
};

const fetch = ({
  filter, limit = 50, orderBy, body,
}) => {
  const lte = timestamp.value;
  timestamp.value = dateHelper().toISOString();
  params.value = {
    projectGroupId: selectedProjectGroup.value?.id,
    filter,
    body,
    orderBy,
    limit,
    timestamp: lte,
  };
};
</script>

<script>
export default {
  name: 'AppActivityList',
};
</script>
