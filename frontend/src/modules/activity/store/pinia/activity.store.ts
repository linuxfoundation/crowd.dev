// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { defineStore } from 'pinia';
import { ref } from 'vue';
import type { Filter } from '@/shared/modules/filters/types/FilterConfig';
import { ActivityService } from '@/modules/activity/activity-service';

export const useActivityStore = defineStore('activity', () => {
  const filters = ref({
    search: '',
    relation: 'and',
    order: {
      prop: 'timestamp',
      order: 'descending',
    },
  } as Filter);
  const activityChannels = ref<Record<string, string[]>>({});

  function fetchActivityChannels(segment?: string) {
    return ActivityService.listActivityChannels(segment).then((res) => {
      activityChannels.value = res;
      return res;
    });
  }

  return {
    filters,
    activityChannels,
    fetchActivityChannels,
  };
});
