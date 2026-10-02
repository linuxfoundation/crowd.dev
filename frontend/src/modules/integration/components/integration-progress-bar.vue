<template>
  <div v-if="props.progress && props.progress.reportStatus !== 'calculating'" class="flex flex-col justify-center w-full h-full">
    <div v-if="!props.barOnly" class="flex justify-between items-center">
      <p class="text-2xs text-gray-500 leading-4 pr-1.5" :class="props.textClass">
        <span v-if="inProgress?.message" v-html="inProgress?.message" />
        <span v-else>Processing still in progress. Please wait for data stream updates.</span>
      </p>

      <el-popover :width="280" placement="top-end">
        <template #reference>
          <div>
            <lf-icon name="circle-info" :size="18" type="regular" class="text-gray-400" :class="props.textClass" />
          </div>
        </template>

        <app-integration-progress :progress="props.progress" />
      </el-popover>
    </div>
    <div v-if="inProgress?.percentage && !props.hideBar" class="rounded-md h-1 bg-gray-200 mt-1.5">
      <div class="rounded-md h-1 bg-primary-800" :style="{ width: `${inProgress?.percentage}%` }" />
    </div>
  </div>
  <div v-else class="flex justify-between items-center h-full">
    <p class="text-2xs text-gray-500 leading-4" :class="props.textClass">
      <span v-if="props.progress">
        Calculating...
      </span>
      <span v-else>
        Loading progress...
      </span>
    </p>
  </div>
</template>

<script setup lang="ts">
import AppIntegrationProgress from '@/modules/integration/components/integration-progress.vue';
import type { IntegrationProgress } from '@/modules/integration/types/IntegrationProgress';
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  progress: IntegrationProgress | null,
  barOnly?: boolean,
  hideBar?: boolean,
  textClass?: string,
}>();

const inProgress = computed(() => {
  if (!props.progress) {
    return null;
  }
  return Object.values(props.progress.data).find((p) => p.status === 'in-progress');
});
</script>

<script lang="ts">
export default {
  name: 'AppIntegrationProgressBar',
};
</script>
