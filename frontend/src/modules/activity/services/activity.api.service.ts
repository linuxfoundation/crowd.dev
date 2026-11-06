// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import authAxios from '@/shared/axios/auth-axios';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { storeToRefs } from 'pinia';
import type { Activity } from '@/shared/modules/activity/types/Activity';
import type { ActivityTypes } from '@/shared/modules/activity/types/ActivityTypes';
import type { Pagination } from '@/shared/types/Pagination';

export interface ActivityQueryBody {
  segments?: string[];
  [key: string]: unknown;
}

const getSelectedProjectGroup = () => {
  const lsSegmentsStore = useLfSegmentsStore();
  const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

  return selectedProjectGroup.value;
};

const query = async (body: ActivityQueryBody, countOnly = false): Promise<Pagination<Activity>> => {
  const response = await authAxios.post(
    '/activity/query',
    {
      ...body,
      countOnly,
      segments: body.segments,
    },
    {
      headers: {
        'x-crowd-api-version': '1',
      },
    },
  );

  return response.data;
};

const listActivityTypes = async (segment?: string): Promise<ActivityTypes> => {
  const segments = segment || getSelectedProjectGroup()?.id;
  const response = await authAxios.get('/activity/type', {
    params: {
      segments: segments ? [segments] : [],
    },
  });

  return response.data;
};

const listActivityChannels = async (segment?: string): Promise<Record<string, string[]>> => {
  const segments = segment || getSelectedProjectGroup()?.id;
  const response = await authAxios.get('/activity/channel', {
    params: {
      segments: segments ? [segments] : [],
    },
  });

  return response.data;
};

export const ActivityService = {
  query,
  listActivityTypes,
  listActivityChannels,
};
