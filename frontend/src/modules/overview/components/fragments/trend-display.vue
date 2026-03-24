<template>
  <lfx-tooltip content="vs. previous 12M period">
    <div class="flex gap-1 items-center">
      <lf-icon
        :name="getTrendIcon"
        type="solid"
        :size="15"
        :class="getTrendColor"
      />
      <span
        v-if="trendDirection !== 'neutral'"
        class="text-xs font-medium text-nowrap"
        :class="getTrendColor"
      >
        {{ trendPercentage }}%
        <span class="hidden sm:inline"> ({{ formatTrendValue }}) </span>
      </span>
      <span
        v-else
        class="text-xs font-medium text-neutral-400"
      >
        0%
      </span>
      <slot name="period">
        <span class="text-xs text-neutral-400">
          {{ data.period }}
        </span>
      </slot>
    </div>
  </lfx-tooltip>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { formatNumber } from '@/utils/number';
import type { OverviewTrends } from '@/modules/admin/modules/overview/types/overview.types';

const props = withDefaults(
  defineProps<{
    data: OverviewTrends;
    decimalPlaces?: number;
  }>(),
  {
    decimalPlaces: 0,
  },
);

const trend = computed(() => props.data.current - props.data.previous);

const trendPercentage = computed(() => {
  if (!props.data.previous) {
    // For division by zero, show as 100% increase
    return '100.0';
  }
  const value = (Math.abs(trend.value) / props.data.previous) * 100;
  return value.toFixed(1);
});

const formatTrendValue = computed(() => {
  const sign = trend.value >= 0 ? '+' : '-';

  return `${sign}${formatNumber(Math.abs(trend.value))}`;
});

const trendDirection = computed(() => {
  if (trend.value === 0) return 'neutral';

  return trend.value > 0 ? 'up' : 'down';
});

const getTrendIcon = computed(() => {
  if (trendDirection.value === 'neutral') return 'equals';

  return trendDirection.value === 'up' ? 'circle-arrow-up' : 'circle-arrow-down';
});

const getTrendColor = computed(() => {
  if (trendDirection.value === 'neutral') return 'text-neutral-400';

  return trendDirection.value === 'up' ? 'text-green-600' : 'text-red-600';
});
</script>

<script lang="ts">
export default {
  name: 'AppLfOverviewTrendDisplay',
};
</script>
