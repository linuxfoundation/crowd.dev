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
        :prevent-auto-open="true"
      />
      <!-- Actions Column -->
      <div class="w-10">
        <lfx-dropdown
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
            v-if="status.key === 'done' && integration.dropdownComponent.dropdownComponent"
            :integration="integration"
            :segment-id="integrationStatus.segmentId"
            :grandparent-id="integrationStatus.grandparentId"
          />
          <lfx-dropdown-item type="danger" @click="disconnectIntegration(integrationStatus)">
            <lf-icon name="link-simple-slash" type="regular" />
            Disconnect integration
          </lfx-dropdown-item>
        </lfx-dropdown>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { getIntegrationStatus } from '@/modules/admin/modules/integration/config/status';
import { lfIntegrations } from '@/config/integrations';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { mapActions } from '@/shared/vuex/vuex.helpers';
import LfxDropdown from '@/ui-kit/lfx/dropdown/dropdown.vue';
import LfxDropdownItem from '@/ui-kit/lfx/dropdown/dropdown-item.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import StatusDisplay from './status-display.vue';
import { IntegrationStatus } from '../../types/overview.types';

const { trackEvent } = useProductTracking();

const { doDestroy } = mapActions('integration');
const props = defineProps<{
  integrationStatus: IntegrationStatus;
  tabKey: string
}>();

const status = computed(() => getIntegrationStatus(props.integrationStatus));
// TODO: Check with Gasper what is the best way to handle this
const integration = computed(() => (props.integrationStatus.platform === 'github-nango'
  ? lfIntegrations(true).github : lfIntegrations()[props.integrationStatus.platform]));

const disconnectIntegration = async (integration: any) => {
  trackEvent({
    key: FeatureEventKey.DISCONNECT_INTEGRATION,
    type: EventType.FEATURE,
    properties: {
      platform: integration.platform,
    },
  });
  await doDestroy(integration.id);

  // TODO: Fetch integrations
};
</script>

<script lang="ts">
export default {
  name: 'AppLfOverviewIntegrationRow',
};
</script>
