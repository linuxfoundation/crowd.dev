<template>
  <div class="pt-6 min-h-120">
    <div class="pb-6 flex justify-between items-center flex-wrap gap-2">
      <lf-tabs v-model="status" :fragment="false">
        <lf-tab v-for="(config, key) in lfIntegrationStatusesTabs" :key="key" :name="key">
          <div class="flex items-center gap-1.5">
            <span>{{ config.tabs.text }}</span>
            <div v-if="integrationStatusCount[key] > 0" class="rounded py-0.5 px-1 text-tiny text-black" :class="config.tabs.badge">
              {{ integrationStatusCount[key] }}
            </div>
          </div>
        </lf-tab>
      </lf-tabs>
      <div class="flex items-center gap-3">
        <lf-switch v-model="lfOnly" size="small">
          <span class="text-small whitespace-nowrap">LF projects only</span>
        </lf-switch>
        <lf-admin-integration-foundation-select v-model="foundation" :is-l-f="lfOnly ? true : null" />
        <lf-admin-integration-platform-select v-model="platform" />
      </div>
    </div>
    <lf-search v-model="query" lazy placeholder="Search sub-projects..." class="!h-9" />
    <div class="pt-5">
      <div v-if="loading && offset === 0" class="pt-3 flex justify-center">
        <lf-spinner />
      </div>
      <div v-else-if="integrations.length > 0">
        <lf-table class="pb-16">
          <thead>
            <tr>
              <lf-table-head class="pl-2">
                Sub-project
              </lf-table-head>
              <lf-table-head>Integration</lf-table-head>
              <lf-table-head class="w-40" />
            </tr>
          </thead>
          <tbody>
            <tr v-for="integration of visibleIntegrations" :key="`${integration.segmentId}-${integration.platform}-${integration.id}`">
              <td class="pl-2">
                <router-link
                  :to="{ name: 'integration', params: { grandparentId: integration.grandparentId, id: integration.segmentId } }"
                  target="_blank"
                  class="group block"
                >
                  <p class="text-medium font-semibold mb-1 group-hover:underline">
                    {{ integration.name }}
                  </p>
                  <p class="text-tiny text-gray-500">
                    {{ hierarchyPath(integration) }}
                  </p>
                </router-link>
              </td>
              <td>
                <div class="flex flex-col gap-1 items-start">
                  <div class="flex items-center gap-1.5">
                    <div>
                      <img
                        :src="integrationConfig(integration.platform)?.image"
                        :alt="integrationConfig(integration.platform)?.name"
                        class="min-w-4 h-4"
                      />
                    </div>
                    <p class="text-medium font-semibold">
                      {{ integrationConfig(integration.platform)?.name }}
                    </p>
                  </div>
                  <div class="text-tiny text-gray-500">
                    <template v-if="status === 'done'">
                      <component
                        :is="integrationConfig(integration.platform)?.connectedParamsComponent"
                        v-if="integrationConfig(integration.platform)?.connectedParamsComponent"
                        :integration="integration"
                        :segment-id="integration.segmentId"
                        :grandparent-id="integration.grandparentId"
                      />
                    </template>
                    <template v-else-if="status === 'connecting'">
                      <app-integration-progress-wrapper :segments="[integration.segmentId]">
                        <template #default="{ progress }">
                          <div class="flex items-center gap-1.5">
                            <lf-icon name="circle-notch" type="solid" class="text-gray-500 animate-spin" :size="16" />
                            <app-integration-progress-bar
                              :progress="progress?.find((p) => p.platform === integration.platform)"
                              hide-bar
                              text-class="!text-tiny"
                            />
                          </div>
                        </template>
                      </app-integration-progress-wrapper>
                    </template>
                    <template v-else-if="status === 'waitingForAction'">
                      <div class="flex items-center gap-1.5">
                        <lf-icon name="triangle-exclamation" type="solid" class="text-yellow-600" :size="16" />
                        <span class="text-tiny text-yellow-600">Action required</span>
                      </div>
                    </template>
                    <template v-else-if="status === 'error'">
                      <div class="flex items-center gap-1.5">
                        <lf-icon name="circle-exclamation" type="solid" class="text-red-600" :size="16" />
                        <span class="text-tiny text-red-600">Connection failed</span>
                      </div>
                    </template>
                    <template v-else-if="status === 'notConnected' && hasConnectGuidance(integration)">
                      <div class="max-w-md">
                        <p class="font-semibold text-gray-600">
                          Before you connect
                        </p>
                        <ul v-if="integrationConfig(integration.platform)?.prerequisites?.length" class="list-disc pl-4">
                          <li v-for="prerequisite of integrationConfig(integration.platform)?.prerequisites" :key="prerequisite">
                            {{ prerequisite }}
                          </li>
                        </ul>
                        <p v-else>
                          {{ integrationConfig(integration.platform)?.description }}
                        </p>
                        <a
                          v-if="integrationConfig(integration.platform)?.link"
                          :href="integrationConfig(integration.platform)?.link"
                          target="_blank"
                          rel="noopener noreferrer"
                          class="text-primary-500 hover:underline"
                        >
                          Setup guide
                        </a>
                      </div>
                    </template>
                  </div>
                </div>
              </td>
              <td>
                <div class="flex justify-end gap-3">
                  <component
                    :is="integrationConfig(integration.platform)?.connectComponent"
                    v-if="status === 'notConnected' && integrationConfig(integration.platform)?.connectComponent"
                    :integration="integration"
                    hide-details
                    :segment-id="integration.segmentId"
                    :grandparent-id="integration.grandparentId"
                  >
                    Connect {{ integrationConfig(integration.platform)?.name }}
                  </component>
                  <component
                    :is="integrationConfig(integration.platform)?.actionComponent"
                    v-if="status === 'waitingForAction' && integrationConfig(integration.platform)?.actionComponent"
                    :integration="integration"
                    :segment-id="integration.segmentId"
                    :grandparent-id="integration.grandparentId"
                  />
                  <template
                    v-if="status !== 'notConnected'
                      && (!integrationConfig(integration.platform)?.autoManaged
                        || (status === 'done' && integrationConfig(integration.platform)?.dropdownComponent))"
                  >
                    <lf-dropdown placement="bottom-end" width="14.5rem" persistent>
                      <template #trigger>
                        <lf-button type="secondary-ghost" icon-only>
                          <lf-icon name="ellipsis" />
                        </lf-button>
                      </template>
                      <component
                        :is="integrationConfig(integration.platform)?.dropdownComponent"
                        v-if="status === 'done' && integrationConfig(integration.platform)?.dropdownComponent"
                        :integration="integration"
                        :segment-id="integration.segmentId"
                        :grandparent-id="integration.grandparentId"
                      />
                      <lf-dropdown-item
                        v-if="!integrationConfig(integration.platform)?.autoManaged"
                        type="danger"
                        @click="disconnectIntegration(integration)"
                      >
                        <lf-icon name="link-simple-slash" type="regular" />
                        Disconnect integration
                      </lf-dropdown-item>
                    </lf-dropdown>
                  </template>
                </div>
              </td>
            </tr>
          </tbody>
        </lf-table>
        <div v-if="integrations.length < total" class="pt-4">
          <lf-button
            type="primary-ghost"
            loading-text="Loading integrations..."
            :loading="loading"
            @click="loadMore()"
          >
            Load more
          </lf-button>
        </div>
      </div>
      <div v-else class="pt-14 flex flex-col items-center">
        <lf-icon name="grid-round" :size="120" class="text-gray-300 mb-6" />
        <h5 class="text-center mb-4">
          {{ lfIntegrationStatusesTabs[status]?.tabs?.empty || 'No integrations' }}
        </h5>
        <p class="text-center text-gray-500 text-small">
          There are no sub-projects with integrations based on your filters criteria.
        </p>
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import {
  computed, onMounted, ref, watch,
} from 'vue';
import LfSearch from '@/ui-kit/search/Search.vue';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import LfTable from '@/ui-kit/table/Table.vue';
import LfTableHead from '@/ui-kit/table/TableHead.vue';
import type { IntegrationStatus } from '@/modules/overview/types/overview.types';
import type { GlobalIntegrationStatusCount } from '@/modules/admin/modules/users/types/GlobalIntegration';
import { UsersService } from '@/modules/admin/modules/users/services/users.service';
import LfAdminIntegrationPlatformSelect
  from '@/modules/admin/modules/integration/components/status/integration-platform-select.vue';
import LfAdminIntegrationFoundationSelect
  from '@/modules/admin/modules/integration/components/status/integration-foundation-select.vue';
import LfSwitch from '@/ui-kit/switch/Switch.vue';
import {
  lfIntegrationStatusesTabs,
} from '@/modules/admin/modules/integration/config/status';
import LfTabs from '@/ui-kit/tabs/Tabs.vue';
import LfTab from '@/ui-kit/tabs/Tab.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import AppIntegrationProgressBar from '@/modules/integration/components/integration-progress-bar.vue';
import AppIntegrationProgressWrapper from '@/modules/integration/components/integration-progress-wrapper.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import { lfIntegrations, type IntegrationConfig } from '@/config/integrations';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import { mapActions } from '@/shared/vuex/vuex.helpers';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';

const loading = ref(true);

const platform = ref('');
const lfOnly = ref(true);
const foundation = ref('');
const status = ref(Object.keys(lfIntegrationStatusesTabs)[0]);
const query = ref<string>('');

const integrations = ref<IntegrationStatus[]>([]);
const offset = ref<number>(0);
const limit = ref<number>(20);
const total = ref<number>(0);

const integrationStatusCount = ref<Record<string, number>>({});

const { doDestroy } = mapActions('integration');

const { trackEvent } = useProductTracking();

const integrationConfig = (integrationPlatform: string): IntegrationConfig | undefined => (integrationPlatform === 'github-nango'
  ? lfIntegrations(true).github : lfIntegrations()[integrationPlatform]);

const visibleIntegrations = computed(() => (status.value === 'notConnected'
  ? integrations.value.filter((integration) => !!integration.platform && !!integrationConfig(integration.platform))
  : integrations.value));

const hasConnectGuidance = (integration: IntegrationStatus) => {
  const config = integrationConfig(integration.platform);
  return !!(config?.prerequisites?.length || config?.description || config?.link);
};

const hierarchyPath = (integration: IntegrationStatus) => [integration.grandparentName, integration.parentName, integration.name]
  .filter((name) => !!name)
  .join(' › ');

const scopeFilters = () => ({
  segment: foundation.value || undefined,
  isLF: lfOnly.value ? true : undefined,
});

const fetchGlobalIntegrations = () => {
  loading.value = true;
  UsersService.fetchGlobalIntegrations({
    limit: limit.value,
    offset: offset.value,
    query: query.value,
    platform: platform.value || undefined,
    status: lfIntegrationStatusesTabs[status.value].statuses,
    ...scopeFilters(),
  })
    .then((res) => {
      if (res.offset > 0) {
        integrations.value = [...integrations.value, ...res.rows];
      } else {
        integrations.value = res.rows;
      }
      total.value = res.count;
    })
    .finally(() => {
      loading.value = false;
    });
};

const fetchGlobalIntegrationStatusCount = () => {
  UsersService.fetchGlobalIntegrationStatusCount({
    platform: platform.value || undefined,
    ...scopeFilters(),
  })
    .then((res) => {
      const statusCount = Object.keys(lfIntegrationStatusesTabs).reduce((acc: Record<string, number>, key) => {
        acc[key] = 0;
        return acc;
      }, {});
      res.forEach((status: GlobalIntegrationStatusCount) => {
        const matchedStatus = Object.values(lfIntegrationStatusesTabs).find((config) => config.show({ status: status.status }));
        if (!matchedStatus) {
          return;
        }
        statusCount[matchedStatus.key] += +status.count;
      });
      integrationStatusCount.value = statusCount;
    });
};

const loadMore = () => {
  offset.value = integrations.value.length;
  fetchGlobalIntegrations();
};

onMounted(() => {
  if (window && window.localStorage) {
    status.value = window.localStorage.getItem('integrationStatusFilter') || Object.keys(lfIntegrationStatusesTabs)[0];
  }
});

watch(() => status.value, () => {
  offset.value = 0;
  fetchGlobalIntegrations();
});

watch(() => lfOnly.value, () => {
  if (foundation.value) {
    foundation.value = '';
    return;
  }
  offset.value = 0;
  fetchGlobalIntegrations();
  fetchGlobalIntegrationStatusCount();
});

watch(() => [platform.value, foundation.value], () => {
  offset.value = 0;
  fetchGlobalIntegrations();
  fetchGlobalIntegrationStatusCount();
});

watch(() => query.value, () => {
  offset.value = 0;
  fetchGlobalIntegrations();
});

onMounted(() => {
  fetchGlobalIntegrations();
  fetchGlobalIntegrationStatusCount();
});

const disconnectIntegration = async (integration: IntegrationStatus) => {
  trackEvent({
    key: FeatureEventKey.DISCONNECT_INTEGRATION,
    type: EventType.FEATURE,
    properties: {
      platform: integration.platform,
    },
  });
  await doDestroy(integration.id);
  fetchGlobalIntegrations();
};
</script>

<script lang="ts">
export default {
  name: 'LfAdminIntegrationStatus',
};
</script>
