<template>
  <section class="py-6 border-t border-gray-200">
    <div class="flex justify-between items-center pb-3">
      <h4 class="text-base font-semibold">
        JSON
      </h4>
      <lf-button type="secondary-ghost" size="small" @click="copy">
        <template v-if="!copied">
          <lf-icon name="copy" />Copy
        </template>
        <template v-else>
          <lf-icon name="circle-check" type="solid" />Copied!
        </template>
      </lf-button>
    </div>
    <pre class="border border-gray-100 bg-gray-50 rounded-md p-4 overflow-auto text-xs">{{ props.log }}</pre>
  </section>
</template>

<script setup lang="ts">
import LfButton from '@/ui-kit/button/Button.vue';
import type { AuditLog } from '@/modules/lf/segments/types/AuditLog';
import { ref } from 'vue';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  log: AuditLog
}>();

const copied = ref(false);

const { trackEvent } = useProductTracking();

const copy = () => {
  if (navigator.clipboard) {
    trackEvent({
      key: FeatureEventKey.COPY_AUDIT_LOG_JSON,
      type: EventType.FEATURE,
    });

    navigator.clipboard.writeText(JSON.stringify(props.log, null, 2));
    copied.value = true;
    setTimeout(() => {
      copied.value = false;
    }, 1000);
  }
};
</script>

<script lang="ts">
export default {
  name: 'AppLfAuditLogsJson',
};
</script>
