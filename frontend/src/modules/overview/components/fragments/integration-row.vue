<template>
  <div
    v-if="integration"
    :key="props.integrationStatus.platform"
    class="flex items-center gap-4 px-5 py-5"
  >
    <!-- Integration Column -->
    <div class="w-1/6 flex items-center gap-1.5">
      <div class="w-4 h-4 flex-shrink-0">
        <img :src="integration.image" :alt="integration.name" class="w-full h-full" />
      </div>
      <span class="text-sm font-medium text-gray-900">{{ integration.name }}</span>
    </div>

    <!-- Project Column -->
    <div class="w-2/6 min-w-0">
      <div class="text-sm font-medium text-gray-900 truncate">
        {{ integrationStatus.name }}
      </div>
      <div class="text-xs text-gray-500 flex items-center min-w-0">
        <span class="truncate flex-shrink min-w-0">{{ integrationStatus.grandparentName }}</span>
        <span class="text-gray-500 flex-shrink-0 px-1">></span>
        <span class="truncate flex-shrink min-w-0">{{ integrationStatus.parentName }}</span>
      </div>
    </div>

    <!-- Status Column -->
    <div class="w-3/6 flex items-center justify-between">
      <slot name="status-display">
        <status-display :integration="integration" :integration-status="integrationStatus" :tab-key="tabKey" />
      </slot>
      <component
        :is="integration.actionComponent"
        v-if="status.key === 'waitingForAction' && integration.actionComponent"
        :integration="integrationStatus"
        :segment-id="integrationStatus.segmentId"
        :grandparent-id="integrationStatus.grandparentId"
        prevent-auto-open
      />
      <!-- Actions Column -->
      <div class="w-10">
        <lfx-dropdown
          v-if="!integration.autoManaged || (status.key === 'done' && integration.dropdownComponent)"
          placement="bottom-end"
          width="14.5rem"
        >
          <template #trigger>
            <lf-button type="secondary-ghost" icon-only>
              <lf-icon name="ellipsis" />
            </lf-button>
          </template>

          <component
            :is="integration.dropdownComponent"
            v-if="status.key === 'done' && integration.dropdownComponent"
            :integration="integration"
            :segment-id="integrationStatus.segmentId"
            :grandparent-id="integrationStatus.grandparentId"
            @open-setting="openSettings"
          />
          <lfx-dropdown-item
            v-if="!integration.autoManaged"
            type="danger"
            @click="disconnectIntegration(integrationStatus)"
          >
            <lf-icon name="link-simple-slash" type="regular" />
            Disconnect integration
          </lfx-dropdown-item>
        </lfx-dropdown>
        <component
          :is="integration.settingComponent"
          v-if="integration.settingComponent && hasOpenedSettings"
          v-model="isSettingsOpen"
          :integration="integrationStatus"
          :segment-id="integrationStatus.segmentId"
          :grandparent-id="integrationStatus.grandparentId"
        />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import {
  computed, nextTick, ref, watch,
} from 'vue';
import { useInvalidate } from '@/shared/queries/invalidate';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { getIntegrationStatus } from '@/modules/admin/modules/integration/config/status';
import { lfIntegrations } from '@/config/integrations';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { useIntegrationStore } from '@/modules/integration/store/integration.store';
import LfxDropdown from '@/ui-kit/lfx/dropdown/dropdown.vue';
import LfxDropdownItem from '@/ui-kit/lfx/dropdown/dropdown-item.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import StatusDisplay from './status-display.vue';
import type { IntegrationStatus } from '../../types/overview.types';

defineOptions({
  name: 'AppLfOverviewIntegrationRow',
});

const props = defineProps<{
  integrationStatus: IntegrationStatus;
  tabKey: string
}>();

const { trackEvent } = useProductTracking();

const integrationStore = useIntegrationStore();

const isSettingsOpen = ref(false);
const hasOpenedSettings = ref(false);
const invalidate = useInvalidate();

const openSettings = async () => {
  hasOpenedSettings.value = true;
  await nextTick();
  isSettingsOpen.value = true;
};

watch(isSettingsOpen, (open) => {
  if (!open) {
    invalidate.globalIntegrations();
  }
});

const status = computed(() => getIntegrationStatus(props.integrationStatus));
// TODO: Check with Gasper what is the best way to handle this
const integration = computed(() => (props.integrationStatus.platform === 'github-nango'
  ? lfIntegrations(true).github : lfIntegrations()[props.integrationStatus.platform]));

const disconnectIntegration = async (integration: IntegrationStatus) => {
  trackEvent({
    key: FeatureEventKey.DISCONNECT_INTEGRATION,
    type: EventType.FEATURE,
    properties: {
      platform: integration.platform,
    },
  });
  await integrationStore.destroy(integration.id);

  // TODO: Fetch integrations
};
</script>
