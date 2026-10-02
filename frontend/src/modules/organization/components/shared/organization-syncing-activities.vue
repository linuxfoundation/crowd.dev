<template>
  <div v-if="props.organization.activitySycning?.state === MergeActionState.IN_PROGRESS" class="mr-4 z-50">
    <lf-tooltip
      placement="bottom"
      :content="`Re-syncing all the activities as this organization was recently
      ${props.organization.activitySycning?.operationType === 'merge' ? 'merged' : 'unmerged'}. This process may take some minutes.`"
    >
      <div class="flex items-center gap-1.5 cursor-default">
        <lf-icon name="circle-notch" :size="16" class="text-secondary-400 animate-spin" />
        <p class="text-small text-secondary-400 font-semibold">
          Syncing activities...
        </p>
      </div>
    </lf-tooltip>
  </div>
</template>

<script setup lang="ts">
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import { MergeActionState } from '@/shared/modules/merge/types/MemberActions';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import type { Organization } from '@/modules/organization/types/Organization';

const props = defineProps<{
  organization: Organization,
}>();
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationSyncingActivities',
};
</script>
