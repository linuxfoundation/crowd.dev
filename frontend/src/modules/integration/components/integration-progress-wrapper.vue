<template>
  <slot :progress="progress" :progress-error="progressError" />
</template>

<script setup lang="ts">
import {
  onMounted, onUnmounted, ref, watch,
} from 'vue';
import { IntegrationService } from '@/modules/integration/integration-service';
import type { IntegrationProgress } from '@/modules/integration/types/IntegrationProgress';
import { useTimeoutPoll } from '@vueuse/core';

const props = withDefaults(defineProps<{
  interval?: number
  segments?: string[]
}>(), {
  interval: 10,
  segments: () => ([]),
});

const progress = ref<IntegrationProgress | null>(null);
const progressError = ref(false);

const fetchUpdates = () => {
  IntegrationService.fetchIntegrationsProgress(props.segments)
    .then((data: IntegrationProgress) => {
      const parsedData = data.platform === 'github-nango' ? {
        ...data,
        platform: 'github',
      } : data;

      progress.value = parsedData;
      if (parsedData.length === 0) {
        pause();
      }
    })
    .catch(() => {
      progress.value = null;
      progressError.value = true;
      pause();
    });
};

const { pause, resume } = useTimeoutPoll(fetchUpdates, props.interval * 1000);

watch(() => props.segments, () => {
  fetchUpdates();
}, { deep: true });

onMounted(() => {
  resume();
});

onUnmounted(() => {
  pause();
});
</script>

<script lang="ts">
export default {
  name: 'AppIntegrationProgressWrapper',
};
</script>
