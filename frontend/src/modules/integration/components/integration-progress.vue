<template>
  <div v-if="props.progress">
    <!-- Header -->
    <div>
      <slot />
    </div>

    <!-- Bar -->
    <div v-if="props.showBar && props.progress?.percentage" class="rounded-md h-1 bg-gray-200">
      <div class="rounded-md h-1 bg-primary-800" :style="{ width: `${props.progress?.percentage}%` }" />
    </div>

    <!-- Details -->
    <div class="flex flex-col gap-2">
      <template v-if="showProgress || props.showParts">
        <div v-for="(part, pi) of parts" :key="pi" class="flex items-center">
          <div v-if="showProgress">
            <lf-icon v-if="part.status === 'ok'" name="circle-check" class="text-green-500 mr-2 flex items-center" />
            <div v-else-if="part.status === 'in-progress'" class="flex items-center justify-center h-4 w-4 mr-2">
              <lf-spinner size="0.75rem" />
            </div>
            <div v-else class="flex items-center justify-center h-4 w-4 mr-2" />
          </div>

          <p class="text-gray-600 text-2xs" v-html="part.message" />
        </div>
      </template>
      <p v-if="!showProgress" class="text-2xs text-gray-400 !whitespace-normal">
        The total number of data streams processed may change during the process.
      </p>
    </div>
  </div>
</template>

<script setup lang="ts">
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import type { IntegrationProgress, IntegrationProgressPart } from '@/modules/integration/types/IntegrationProgress';
import { computed } from 'vue';
import { lfIntegrations } from '@/config/integrations';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  progress: IntegrationProgress | null,
  showBar?: boolean,
  showParts?: boolean,
}>();

const statusPriority = {
  ok: 1,
  'in-progress': 2,
};

const priority = (status: string) => statusPriority[status] || (Object.keys(statusPriority).length + 1);

const showProgress = computed(() => {
  if (!props.progress) {
    return false;
  }
  return lfIntegrations()[props.progress.platform]?.showProgress || false;
});

const parts = computed<IntegrationProgressPart[]>(() => {
  if (!props.progress?.data) {
    return [];
  }

  return Object.values(props.progress.data).sort((a, b) => priority(a.status) - priority(b.status));
});
</script>

<script lang="ts">
export default {
  name: 'AppIntegrationProgress',
};
</script>
