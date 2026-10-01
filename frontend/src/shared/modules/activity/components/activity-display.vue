<template>
  <div>
    <component
      :is="configuration?.activityHeaderContent"
      v-if="!inProfile"
      :activity="activity"
      :in-profile="inProfile"
      :in-dashboard="inDashboard"
    />
    <component :is="configuration?.activityContent" :activity="activity" />
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import config from '@/modules/activity/config/display/main';
import type { Activity } from '../types/Activity';

defineProps<{
  activity: Activity;
  inProfile?: boolean;
  inDashboard?: boolean;
}>();

const configuration = computed(() => config.git);
</script>
