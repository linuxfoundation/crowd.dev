<template>
  <slot :progress="progress" :progress-error="progressError" />
</template>

<script setup lang="ts">
import {
  onMounted, onUnmounted, ref, watch,
} from 'vue';
import { IntegrationService } from '@/modules/integration/services/integration.api.service';
import type { IntegrationProgress } from '@/modules/integration/types/IntegrationProgress';
import { useTimeoutPoll } from '@vueuse/core';

defineOptions({
  name: 'AppIntegrationProgressWrapper',
});

const props = withDefaults(defineProps<{
  interval?: number
  segments?: string[]
}>(), {
  interval: 10,
  segments: () => ([]),
});

const progress = ref<IntegrationProgress[] | null>(null);
const progressError = ref(false);

const fetchUpdates = () => {
  IntegrationService.fetchIntegrationsProgress(props.segments)
    .then((data: IntegrationProgress[]) => {
      progress.value = data;
      if (data.length === 0) {
        // eslint-disable-next-line no-use-before-define -- pause comes from useTimeoutPoll(fetchUpdates) below
        pause();
      }
    })
    .catch(() => {
      progress.value = null;
      progressError.value = true;
      // eslint-disable-next-line no-use-before-define -- pause comes from useTimeoutPoll(fetchUpdates) below
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
