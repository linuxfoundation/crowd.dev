<template>
  <lf-tooltip
    :content="props.tooltipContent"
    :disabled="!props.tooltipContent"
    placement="top"
    class="font-primary font-semibold"
  >
    <lfx-chip
      type="bordered"
      size="default"
      class="cursor-default"
    >
      <span class="text-xs text-gray-500 font-semibold font-primary">
        {{ props.version }}
      </span>
    </lfx-chip>
  </lf-tooltip>
</template>

<script setup lang="ts">
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import LfxChip from '@/ui-kit/lfx/chip/chip.vue';

const props = defineProps<{
  version: 'v1' | 'v2';
  tooltipContent?: string;
}>();
</script>
