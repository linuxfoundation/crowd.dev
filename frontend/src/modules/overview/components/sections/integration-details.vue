<template>
  <div class="bg-white border border-gray-200 rounded-lg shadow-sm">
    <!-- Header -->
    <div class="border-b border-gray-200 px-5 pt-5 pb-0">
      <h3 class="text-base font-semibold text-gray-900 mb-6">
        In progress & troubleshooting
      </h3>

      <!-- Tabs -->
      <integration-tabs
        v-if="!isEmpty"
        :tabs="overviewTabs.filter((tab) => tab.count > 0)"
        :model-value="activeTab"
        @update:model-value="activeTab = $event"
      />
    </div>

    <template v-if="isPending">
      <div class="flex items-center justify-center py-10 h-80">
        <lf-spinner size="1rem" />
      </div>
    </template>
    <template v-else>
      <template v-if="isEmpty">
        <div class="flex flex-col items-center justify-center py-15 h-80">
          <lf-icon name="plug-circle-check" :size="64" class="text-gray-300 mb-6" />
          <h5 class="text-center mb-3 text-base">
            Everything looks good
          </h5>
          <p class="text-center text-gray-500 text-small">
            No integrations are currently in progress, requiring action, or have failed connections according to your filter criteria.
          </p>
        </div>
      </template>
      <template v-else>
        <!-- Table Headers -->
        <div class="flex items-center gap-4 pt-5 pb-3 px-5 text-xs font-semibold text-gray-400">
          <div class="w-1/6">
            Integration
          </div>
          <div class="w-2/6">
            Project
          </div>
          <div class="w-3/6">
            Status
          </div>
        </div>

        <!-- Integration Rows -->
        <div class="divide-y divide-gray-200">
          <integration-row
            v-for="integrationStatus in paginatedIntegrations"
            :key="integrationStatus.id"
            :integration-status="integrationStatus"
            :tab-key="activeTab"
          />
        </div>

        <!-- Footer -->
        <div class="px-5 py-6 flex items-center justify-center gap-4">
          <span class="text-xs text-gray-400">{{ paginationText }}</span>
          <lf-button
            v-if="hasNextPage"
            type="primary-ghost"
            :disabled="isFetchingNextPage"
            :class="{ 'cursor-not-allowed': isFetchingNextPage }"
            @click="() => fetchNextPage()"
          >
            <lf-icon v-if="isFetchingNextPage" name="circle-notch animate-spin" :size="16" class="text-primary-200" />
            <span v-if="isFetchingNextPage" class="text-primary-200">Loading integrations...</span>
            <span v-else>Load more</span>
          </lf-button>
        </div>
      </template>
    </template>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue';
import { storeToRefs } from 'pinia';
import { lfIntegrationStatusesTabs } from '@/modules/admin/modules/integration/config/status';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import { ToastStore } from '@/shared/message/notification';
import LfButton from '@/ui-kit/button/Button.vue';
import { OVERVIEW_API_SERVICE } from '../../services/overview.api.service';
import { useOverviewStore } from '../../store/overview.store';
import IntegrationRow from '../fragments/integration-row.vue';
import IntegrationTabs from '../fragments/integration-tabs.vue';

const {
  integrationStatusCount,
  selectedIntegrationId,
  selectedSubProjectId,
  selectedProjectId,
  selectedProjectGroupId,
} = storeToRefs(useOverviewStore());

const activeTab = ref(lfIntegrationStatusesTabs.connecting.key);
const limit = ref(50);

const overviewTabs = computed(() => [
  {
    label: 'In progress',
    key: lfIntegrationStatusesTabs.connecting.key,
    count: integrationStatusCount.value[lfIntegrationStatusesTabs.connecting.key] || 0,
    icon: lfIntegrationStatusesTabs.connecting.chipStatus?.icon || lfIntegrationStatusesTabs.connecting.status.icon,
  },
  {
    label: 'Action required',
    key: lfIntegrationStatusesTabs.waitingForAction.key,
    count: integrationStatusCount.value[lfIntegrationStatusesTabs.waitingForAction.key] || 0,
    icon: lfIntegrationStatusesTabs.waitingForAction.chipStatus?.icon || lfIntegrationStatusesTabs.waitingForAction.status.icon,
  },
  {
    label: 'Connection failed',
    key: lfIntegrationStatusesTabs.error.key,
    count: integrationStatusCount.value[lfIntegrationStatusesTabs.error.key] || 0,
    icon: lfIntegrationStatusesTabs.error.chipStatus?.icon || lfIntegrationStatusesTabs.error.status.icon,
  },
]);

const params = computed(() => ({
  platform: selectedIntegrationId.value || undefined, // activeTab.value,
  status: lfIntegrationStatusesTabs[activeTab.value].statuses,
  query: '',
  limit: limit.value,
  segment: selectedSubProjectId.value || selectedProjectId.value || selectedProjectGroupId.value || undefined,
}));

const {
  data, isError, isPending, isFetchingNextPage, fetchNextPage, hasNextPage,
} = OVERVIEW_API_SERVICE.fetchGlobalIntegrations(params);

// @ts-expect-error - TanStack Query type inference issue with Vue
const totalCount = computed(() => data.value?.pages[0].count || 0);

// @ts-expect-error - TanStack Query type inference issue with Vue
const paginatedIntegrations = computed(() => data.value?.pages.flatMap((page) => page.rows) || []);

const isEmpty = computed(() => overviewTabs.value.every((tab) => tab.count === 0));

const paginationText = computed(() => {
  const showing = paginatedIntegrations.value.length;
  return `${showing} out of ${totalCount.value} integrations`;
});

watch(isError, () => {
  if (isError.value) {
    ToastStore.error('Failed to fetch global integrations');
  }
});

watch(overviewTabs, (newValue) => {
  // Loop through the integrationStatusCount and if the count is 0, set the activeTab to the next tab
  let activeTabKey = '';
  newValue.forEach((tab) => {
    if (tab.count > 0 && !activeTabKey) {
      activeTabKey = tab.key;
    }
  });

  if (activeTabKey) {
    activeTab.value = activeTabKey;
  }
}, { immediate: true, deep: true });
</script>

<script lang="ts">
export default {
  name: 'AppLfOverviewIntegrationDetails',
};
</script>
