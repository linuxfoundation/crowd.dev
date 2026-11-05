// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { defineStore } from 'pinia';
import { ref } from 'vue';
import type { Filter } from '@/shared/modules/filters/types/FilterConfig';
import { ActivityService } from '@/modules/activity/activity-service';
import type { Activity } from '@/shared/modules/activity/types/Activity';
import { dateHelper } from '@/shared/date-helper/date-helper';

interface ActivityQueryResult {
  rows: Activity[];
  count: number;
}

export const useActivityStore = defineStore('activity', () => {
  const filters = ref({
    search: '',
    relation: 'and',
    order: {
      prop: 'timestamp',
      order: 'descending',
    },
  } as Filter);
  const limit = ref(20);
  const timestamp = ref(dateHelper().toISOString());
  const savedFilterBody = ref<Record<string, unknown>>({});
  const activities = ref<Activity[]>([]);
  const totalActivities = ref(0);
  const activityChannels = ref<Record<string, string[]>>({});

  function fetchActivities({
    body = {},
    reload = false,
    append = false,
  }: {
    body?: Record<string, unknown>;
    reload?: boolean;
    append?: boolean;
  }): Promise<ActivityQueryResult> {
    const mappedBody = reload ? savedFilterBody.value : body;

    if (!append) {
      activities.value = [];
      timestamp.value = dateHelper().toISOString();
    }
    return ActivityService.query(mappedBody)
      .then((data: ActivityQueryResult) => {
        if (append) {
          activities.value = activities.value.concat(
            ...data.rows.filter(
              (row) => !activities.value.some((activity) => activity.id === row.id),
            ),
          );
        } else {
          activities.value = data.rows;
        }

        totalActivities.value = data.count;
        savedFilterBody.value = mappedBody;
        return data;
      })
      .catch((err) => {
        activities.value = [];
        totalActivities.value = 0;
        throw err;
      });
  }

  function fetchActivityChannels(segment: string) {
    return ActivityService.listActivityChannels(segment).then((res) => {
      activityChannels.value = res;
      return res;
    });
  }

  return {
    filters,
    limit,
    timestamp,
    savedFilterBody,
    activities,
    totalActivities,
    activityChannels,
    fetchActivities,
    fetchActivityChannels,
  };
});
