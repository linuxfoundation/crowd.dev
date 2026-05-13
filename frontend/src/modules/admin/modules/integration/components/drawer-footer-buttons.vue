<template>
  <div
    class="flex"
    :class="{ 'justify-between': isEditMode, 'justify-end': !isEditMode }"
  >
    <template v-if="isEditMode">
      <lf-tooltip
        v-if="disconnectTooltipContent"
        :content="disconnectTooltipContent"
        :disabled="!isDisconnectDisabled"
        placement="top"
        class="font-primary font-semibold"
        content-class="!w-100"
      >
        <lf-button
          type="danger-ghost"
          :disabled="isDisconnectDisabled"
          @click="isDisconnectIntegrationModalOpen = true"
        >
          <lf-icon name="link-simple-slash" :size="16" />
          Disconnect
        </lf-button>
      </lf-tooltip>
      <lf-button
        v-else
        type="danger-ghost"
        @click="isDisconnectIntegrationModalOpen = true"
      >
        <lf-icon name="link-simple-slash" :size="16" />
        Disconnect
      </lf-button>
    </template>

    <span class="flex gap-3">
      <lf-button
        v-if="!isEditMode"
        type="outline"
        :disabled="isLoading"
        @click="cancel"
      >
        Cancel
      </lf-button>
      <lf-button
        v-if="hasFormChanged && isEditMode"
        type="outline"
        @click="revertChanges()"
      >
        <lf-icon name="arrow-rotate-left" :size="16" />
        Revert changes
      </lf-button>
      <lf-tooltip
        v-if="connectTooltipContent"
        :content="connectTooltipContent"
        content-class="!w-50"
        placement="top-end"
        :disabled="!isSubmitDisabled"
      >
        <lf-button
          id="gitConnect"
          type="primary"
          class="!rounded-full"
          :disabled="isSubmitDisabled"
          :loading="isLoading"
          @click="connect"
        >
          <lf-icon v-if="!isEditMode" name="link-simple" :size="16" />
          {{ isEditMode ? 'Update' : 'Connect' }}
        </lf-button>
      </lf-tooltip>
      <lf-button
        v-else
        id="gitConnect"
        type="primary"
        class="!rounded-full"
        :disabled="isSubmitDisabled"
        :loading="isLoading"
        @click="connect"
      >
        <lf-icon v-if="!isEditMode" name="link-simple" :size="16" />
        {{ isEditMode ? 'Update' : 'Connect' }}
      </lf-button>
    </span>
  </div>

  <integration-confirmation-modal
    v-if="props.integration"
    v-model="isDisconnectIntegrationModalOpen"
    :platform="props.integration.platform"
    :integration-id="props.integration.id"
  />
</template>

<script setup lang="ts">
import { ref } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import IntegrationConfirmationModal from '@/modules/admin/modules/integration/components/integration-confirmation-modal.vue';

const props = defineProps<{
  integration: any;
  isEditMode: boolean;
  hasFormChanged: boolean;
  isLoading: boolean;
  isSubmitDisabled: boolean;
  isDisconnectDisabled?: boolean;
  connectTooltipContent?: string;
  disconnectTooltipContent?: string;
  cancel:() => void;
  revertChanges: () => void;
  connect: () => void;
}>();

const isDisconnectIntegrationModalOpen = ref(false);
</script>

<script lang="ts">
export default {
  name: 'LfDrawerFooterButtons',
};
</script>
