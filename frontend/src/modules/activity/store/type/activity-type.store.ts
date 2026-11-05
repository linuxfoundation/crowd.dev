import { defineStore } from 'pinia';
import { ref } from 'vue';
import { ActivityService } from '@/modules/activity/activity-service';
import type { ActivityTypes } from '@/shared/modules/activity/types/ActivityTypes';

export const useActivityTypeStore = defineStore('activity-type', () => {
  const types = ref<ActivityTypes>({ default: {}, custom: {} });

  function fetchActivityTypes(segment?: string): Promise<ActivityTypes> {
    return ActivityService.listActivityTypes(segment)
      .then((response: ActivityTypes) => {
        types.value = response;
        return response;
      });
  }

  return {
    types,
    fetchActivityTypes,
  };
});
