<template>
  <lf-modal v-model="isModalOpen">
    <template #default>
      <div class="px-6 pt-6 flex gap-4">
        <div class="min-w-10 w-10 h-10 rounded-full bg-red-50 flex items-center justify-center">
          <lf-icon name="link-simple-slash" class="text-red-500" :size="16" />
        </div>
        <div class="flex flex-col gap-6">
          <div class="flex flex-col gap-2">
            <span class="font-semibold">Are you sure you want to disconnect this integration?</span>
            <p class="text-gray-500 text-small">
              Once disconnected, data will no longer sync from this source.
              You can reconnect anytime to resume syncing, but this action can’t be undone.
            </p>
          </div>
          <div class="flex flex-col gap-1">
            <span class="font-semibold">Type DISCONNECT to confirm</span>
            <lf-input v-model="disconnectConfirm" placeholder="DISCONNECT" class="w-full" />
          </div>
        </div>
      </div>
      <div class="px-6 py-4.5 bg-gray-50 mt-8 flex justify-end gap-4">
        <lf-button type="outline" @click="isModalOpen = false">
          Cancel
        </lf-button>
        <lf-button type="danger" class="!rounded-full" :disabled="disconnectConfirm !== 'DISCONNECT'" @click="disconnectIntegration()">
          Disconnect integration
        </lf-button>
      </div>
    </template>
  </lf-modal>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import LfModal from '@/ui-kit/modal/Modal.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfInput from '@/ui-kit/input/Input.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { mapActions } from '@/shared/vuex/vuex.helpers';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';

const { doDestroy } = mapActions('integration');
const { trackEvent } = useProductTracking();

const props = defineProps<{
  modelValue: boolean;
  platform: string;
  integrationId: string;
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void}>();

const disconnectConfirm = ref('');

const isModalOpen = computed({
  get: () => props.modelValue,
  set: (value: boolean) => emit('update:modelValue', value),
});

watch(isModalOpen, (open) => {
  if (!open) {
    disconnectConfirm.value = '';
  }
});

const disconnectIntegration = () => {
  isModalOpen.value = false;
  trackEvent({
    key: FeatureEventKey.DISCONNECT_INTEGRATION,
    type: EventType.FEATURE,
    properties: {
      platform: props.platform,
    },
  });
  doDestroy(props.integrationId);
};

</script>

<script lang="ts">
export default {
  name: 'IntegrationConfirmationModal',
};
</script>
