<template>
  <div v-if="inProgress" class="mr-4 z-50">
    <lf-tooltip
      placement="bottom"
      :content="`Re-syncing all the activities as this profile was recently
      ${ props.contributor.activitySycning?.operationType === 'merge' ? 'merged' : 'unmerged' }. This process may take some minutes.`"
    >
      <div class="flex items-center gap-1.5 cursor-default">
        <lf-icon name="circle-notch" type="solid" :size="16" class="text-secondary-400 animate-spin" />
        <p class="text-small text-secondary-400 font-semibold">
          Syncing activities...
        </p>
      </div>
    </lf-tooltip>
  </div>
</template>

<script setup lang="ts">
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import { computed } from 'vue';
import { MergeActionState } from '@/shared/modules/merge/types/MemberActions';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  contributor: Contributor,
}>();

const inProgress = computed(() => props.contributor.activitySycning?.state === MergeActionState.IN_PROGRESS);
</script>

<script lang="ts">
export default {
  name: 'LfContributorSyncingActivities',
};
</script>
