<template>
  <lf-tooltip
    v-if="!!props.organization.lfxMembership"
    content="Active member organization of the LF or one of its projects"
  >
    <div class="text-primary-500 flex items-center gap-1 flex-nowrap">
      <lf-icon name="circle-bookmark" />
      <span class="font-semibold text-tiny whitespace-nowrap" v-bind="$attrs">Member</span>
    </div>
  </lf-tooltip>
</template>

<script setup lang="ts">
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import type { Organization } from '../../types/Organization';

const props = withDefaults(
  defineProps<{
      organization: Organization;
      iconOnly?: boolean;
      iconSize?: number;
    }>(),
  {
    iconOnly: false,
    iconSize: 16,
  },
);
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationMembership',
};
</script>
