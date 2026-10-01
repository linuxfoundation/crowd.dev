<template>
  <div v-if="props.contributor.activitySycning?.state === MergeActionState.IN_PROGRESS" class="pt-12 flex flex-col items-center">
    <lf-icon name="circle-notch" type="solid" :size="40" class="text-gray-300 animate-spin" />
    <h6 class="text-center py-3">
      Syncing activities...
    </h6>
    <p class="text-center text-medium text-gray-500">
      Re-syncing all the activities as this person was recently
      {{ props.contributor.activitySycning?.operationType === 'merge' ? 'merged' : 'unmerged' }}.<br>
      This process may take some minutes.
    </p>
  </div>
  <div v-else-if="props.contributor.activitySycning?.state === MergeActionState.ERROR" class="pt-12 flex flex-col items-center">
    <lf-icon name="circle-exclamation" :size="40" class="text-gray-300" />
    <h6 class="text-center py-3">
      Error syncing activities
    </h6>
    <p class="text-center text-medium text-gray-500">
      An error occurred while syncing this profile activities.<br>
      Please contact our support team.
    </p>
  </div>
  <app-activity-timeline
    v-else
    ref="timeline"
    :entity="props.contributor"
    entity-type="member"
    :show-affiliations="true"
    :selected-segment="subProjectId || null"
    class="max-w-full"
  />
</template>

<script setup lang="ts">
import type { Contributor } from '@/modules/contributor/types/Contributor';
import AppActivityTimeline from '@/modules/activity/components/activity-timeline.vue';
import { useRoute } from 'vue-router';
import { MergeActionState } from '@/shared/modules/merge/types/MemberActions';
import { ref } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  contributor: Contributor,
}>();

const route = useRoute();

const timeline = ref(null);

const { subProjectId } = route.query;

const loadMore = () => {
  timeline.value.fetchActivities();
};

defineExpose({
  loadMore,
});
</script>

<script lang="ts">
export default {
  name: 'LfContributorDetailsActivities',
};
</script>
