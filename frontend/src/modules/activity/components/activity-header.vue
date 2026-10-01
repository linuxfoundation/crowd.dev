<template>
  <div>
    <slot>
      <app-activity-message
        :activity="activity"
        class="font-medium"
      />
    </slot>
    <div class="whitespace-nowrap flex items-center">
      <div v-if="showAffiliations && activity.organization" class="flex items-center">
        <span class="mx-1 text-gray-500">·</span>
        <router-link
          :to="{
            name: 'organizationView',
            params: {
              id: activity.organization.id,
            },
            query: {
              projectGroup: selectedProjectGroup?.id,
              segmentId,
            },
          }"
          class="group hover:cursor-pointer"
        >
          <div class="flex items-center gap-1">
            <img
              v-if="activity.organization.logo"
              class="min-w-3.5 h-3.5"
              :src="activity.organization.logo"
              :alt="`${activity.organization.displayName} logo`"
            />
            <span
              class="text-gray-500 group-hover:decoration-gray-900
             transition text-xs underline decoration-dashed underline-offset-4 decoration-gray-400"
            >{{ activity.organization.displayName }}</span>
            <lf-organization-lf-member-tag
              :organization="activity.organization"
              only-show-icon
            />
          </div>
        </router-link>
      </div>
      <span class="mx-1 text-gray-500">·</span>
      <span class="text-gray-500">{{ timeAgo }}</span>
    </div>
    <span
      v-if="sentiment"
      class="mx-1"
    >·</span>
    <app-activity-sentiment
      v-if="sentiment"
      :sentiment="sentiment"
    />
  </div>
</template>

<script setup>
import { computed } from 'vue';
import AppActivityMessage from '@/modules/activity/components/activity-message.vue';
import AppActivitySentiment from '@/modules/activity/components/activity-sentiment.vue';
import { formatDateToTimeAgo } from '@/utils/date';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import LfOrganizationLfMemberTag from '@/modules/organization/components/lf-member/organization-lf-member-tag.vue';
import { useActivityStore } from '../store/pinia';

const props = defineProps({
  activity: {
    type: Object,
    default: () => {},
  },
  showAffiliations: {
    type: Boolean,
    default: true,
  },
});

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const activityStore = useActivityStore();
const { filters } = storeToRefs(activityStore);

const segmentId = computed(() => {
  if (!filters.value.projects) {
    return selectedProjectGroup.value?.id;
  }

  return filters.value.projects.value[0];
});
const timeAgo = computed(() => formatDateToTimeAgo(props.activity.timestamp));
const sentiment = computed(() => props.activity?.sentiment?.sentiment || 0);
</script>

<script>
export default {
  name: 'AppActivityHeader',
};
</script>
