<template>
  <div class="flex flex-col gap-1">
    <template v-if="tabKey === 'connecting'">
      <app-integration-progress-wrapper :segments="[integrationStatus.segmentId]">
        <template #default="{ progress }">
          <div class="flex flex-col gap-1">
            <div class="flex items-center gap-1.5">
              <lf-icon name="circle-notch" type="solid" class="text-primary-500 animate-spin" :size="16" />
              <span class="text-sm font-medium">In progress</span>
            </div>
            <app-integration-progress-bar
              :progress="progress.find((p) => p.platform === integrationStatus.platform)"
              :hide-bar="true"
              text-class="!text-tiny"
            />
          </div>
        </template>
      </app-integration-progress-wrapper>
    </template>
    <template v-else>
      <div class="flex items-center gap-1.5 mb-1">
        <lf-icon
          :name="status.status.icon"
          :size="16"
          :type="status.status.iconType"
          :class="status.status.color"
        />
        <span class="text-sm font-medium text-gray-900">{{ status.status.text }}</span>
      </div>
      <div v-if="tabKey === 'waitingForAction'" class="text-xs text-gray-500">
        {{ actionRequiredMessage }}
      </div>
      <div v-else class="text-xs text-gray-500">
        {{ integrationStatus.statusDetails }}
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { getIntegrationStatus } from '@/modules/admin/modules/integration/config/status';
import AppIntegrationProgressWrapper from '@/modules/integration/components/integration-progress-wrapper.vue';
import AppIntegrationProgressBar from '@/modules/integration/components/integration-progress-bar.vue';
import { IntegrationConfig } from '@/config/integrations';
import { IntegrationStatus } from '../../types/overview.types';

const props = defineProps<{
  integration:IntegrationConfig;
  integrationStatus: IntegrationStatus;
  tabKey: string;
}>();

// See https://linuxfoundation.atlassian.net/browse/CM-846 for more details on the action required messages

const status = computed(() => getIntegrationStatus(props.integrationStatus));

const actionRequiredMessage = computed(() => {
  const message = props.integration.actionRequiredMessage?.find((message) => message.key === props.integrationStatus.status);
  return message?.text || '';
});
</script>
