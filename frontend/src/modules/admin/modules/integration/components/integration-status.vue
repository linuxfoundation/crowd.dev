<template>
  <div class="flex items-center text-medium gap-1.5 whitespace-nowrap">
    <template v-if="status">
      <lf-icon v-if="status.status.icon" :name="status.status.icon" :type="status.status.iconType" :class="status.status.color" :size="20" />
      <span class="font-semibold" :class="status.status.color">{{ status.status.text }}</span>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import type { IntegrationStatusSource } from '@/modules/admin/modules/integration/config/status';
import { getIntegrationStatus } from '@/modules/admin/modules/integration/config/status';

const props = defineProps<{
  integration: IntegrationStatusSource,
}>();

const status = computed(() => getIntegrationStatus(props.integration));

</script>

<script lang="ts">
export default {
  name: 'LfIntegrationStatus',
};
</script>
