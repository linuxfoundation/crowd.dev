<template>
  <div v-if="isPending" class="flex items-center justify-center h-20">
    <lf-spinner />
  </div>
  <div v-else class="flex gap-4">
    <!-- TODO: revisit this implementation when the backend is ready -->
    <!-- we may use loop to render the cards -->
    <lf-card v-if="!selectedProject" class="flex-1 p-4 flex flex-col gap-2">
      <div class="flex items-center justify-between">
        <span class="text-xs font-semibold">Sub-projects</span>
        <lf-icon name="folders" type="light" class="text-gray-400" />
      </div>
      <div class="text-xl font-light">
        {{ formatNumber(projectsTrends.current) }}
      </div>
      <app-lf-overview-trend-display :data="projectsTrends" />
    </lf-card>
    <lf-card class="flex-1 p-4 flex flex-col gap-2">
      <div class="flex items-center justify-between">
        <span class="text-xs font-semibold">People</span>
        <lf-icon name="people-group" type="light" class="text-gray-400" />
      </div>
      <div class="text-xl font-light">
        {{ formatNumber(peopleTrends.current) }}
      </div>
      <app-lf-overview-trend-display :data="peopleTrends" />
    </lf-card>
    <lf-card class="flex-1 p-4 flex flex-col gap-2">
      <div class="flex items-center justify-between">
        <span class="text-xs font-semibold">Organizations</span>
        <lf-icon name="building" type="light" class="text-gray-400" />
      </div>
      <div class="text-xl font-light">
        {{ formatNumber(organizationsTrends.current) }}
      </div>
      <app-lf-overview-trend-display :data="organizationsTrends" />
    </lf-card>
    <lf-card class="flex-1 p-4 flex flex-col gap-2">
      <div class="flex items-center justify-between">
        <span class="text-xs font-semibold">Activities</span>
        <lf-icon name="monitor-waveform" type="light" class="text-gray-400" />
      </div>
      <div class="text-xl font-light">
        {{ formatNumber(activitiesTrends.current) }}
      </div>
      <app-lf-overview-trend-display :data="activitiesTrends" />
    </lf-card>
  </div>
</template>

<script setup lang="ts">
import { computed, watch } from 'vue';
import LfCard from '@/ui-kit/card/Card.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { storeToRefs } from 'pinia';
import { ToastStore } from '@/shared/message/notification';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import { formatNumber } from '@/utils/number';
import type { OverviewTrends } from '../../types/overview.types';
import AppLfOverviewTrendDisplay from '../fragments/trend-display.vue';
import { OVERVIEW_API_SERVICE } from '../../services/overview.api.service';
import { useOverviewStore } from '../../store/overview.store';

const overviewStore = useOverviewStore();
const {
  selectedProject, selectedProjectGroupId, selectedProjectId, selectedSubProjectId,
} = storeToRefs(overviewStore);

const params = computed(() => ({
  segment: selectedSubProjectId.value || selectedProjectId.value || selectedProjectGroupId.value || undefined,
}));

const { data, isPending, isError } = OVERVIEW_API_SERVICE.fetchDashboardMetrics(params);
const projectsTrends = computed<OverviewTrends>(() => ({
  current: Number(data.value?.projectsTotal || 0),
  previous: Number((data.value?.projectsTotal || 0) - Number(data.value?.projectsLast30Days || 0)),
  period: 'vs. last 30d',
}));

const peopleTrends = computed<OverviewTrends>(() => ({
  current: Number(data.value?.membersTotal || 0),
  previous: Number((data.value?.membersTotal || 0) - Number(data.value?.membersLast30Days || 0)),
  period: 'vs. last 30d',
}));

const organizationsTrends = computed<OverviewTrends>(() => ({
  current: Number(data.value?.organizationsTotal || 0),
  previous: Number((data.value?.organizationsTotal || 0) - Number(data.value?.organizationsLast30Days || 0)),
  period: 'vs. last 30d',
}));

const activitiesTrends = computed<OverviewTrends>(() => ({
  current: Number(data.value?.activitiesTotal || 0),
  previous: Number((data.value?.activitiesTotal || 0) - Number(data.value?.activitiesLast30Days || 0)),
  period: 'vs. last 30d',
}));

watch(isError, (newVal) => {
  if (newVal) {
    ToastStore.error('Failed to fetch dashboard metrics');
  }
}, { immediate: true });
</script>

<script lang="ts">
export default {
  name: 'AppLfOverviewSummary',
};
</script>
