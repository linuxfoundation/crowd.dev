<template>
  <lf-pill :type="statusDisplay[props.status]?.type || 'primary'">
    {{ statusDisplay[props.status]?.label }}
  </lf-pill>
</template>

<script lang="ts" setup>
import LfPill from '@/ui-kit/pill/Pill.vue';
import type { PillType } from '@/ui-kit/pill/types/PillType';

const props = defineProps<{
  status: string;
}>();

const statusDisplay: Record<string, { type: PillType, label: string }> = {
  active: {
    type: 'success',
    label: 'Active',
  },
  formation: {
    type: 'primary',
    label: 'Formation',
  },
  prospect: {
    type: 'warning',
    label: 'Prospect',
  },
  archived: {
    type: 'secondary',
    label: 'Archived',
  },
};
</script>

<script lang="ts">
export default {
  name: 'LfProjectStatusPill',
};
</script>
