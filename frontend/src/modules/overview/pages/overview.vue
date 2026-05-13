<template>
  <div class="flex flex-col gap-10 lg:px-8 px-6 max-w-[1200px] w-full mx-auto">
    <div class="flex items-center justify-between pt-1 pb-5 border-b border-gray-200">
      <h4>
        Overview
      </h4>
      <div>
        <app-lf-overview-filter />
      </div>
    </div>
    <app-lf-overview-summary />
    <app-lf-overview-integration-status />
    <app-lf-overview-integration-details />
  </div>
</template>

<script setup lang="ts">
import AppLfOverviewSummary from '@/modules/overview/components/sections/summary.vue';
import AppLfOverviewIntegrationStatus from '@/modules/overview/components/sections/integration-status.vue';
import AppLfOverviewIntegrationDetails from '@/modules/overview/components/sections/integration-details.vue';
import AppLfOverviewFilter from '@/modules/overview/components/sections/overview-filter.vue';
</script>

<script lang="ts">
export default {
  name: 'AppLfOverviewPage',
};
</script>
