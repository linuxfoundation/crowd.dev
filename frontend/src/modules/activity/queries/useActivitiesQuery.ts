// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { computed, type Ref } from 'vue';
import { useInfiniteQuery } from '@tanstack/vue-query';
import { ActivityService } from '@/modules/activity/services/activity.api.service';
import type { Activity } from '@/shared/modules/activity/types/Activity';
import { queryKeys } from '@/shared/queries/keys';

export interface ActivitiesQueryParams {
  projectGroupId?: string;
  filter: Record<string, unknown>;
  body: Record<string, unknown>;
  orderBy: string;
  limit: number;
  timestamp: string;
}

interface ActivitiesPage {
  rows: Activity[];
  count: number;
}

const LOAD_MORE_LIMIT = 20;

const lteFilter = (lte: string) => ({ timestamp: { lte } });

// Load-more requests drop the in-body filters and use a smaller limit, as the store did.
const buildBody = (params: ActivitiesQueryParams, cursors: string[]) => {
  const filter = {
    ...params.filter,
    and: [
      ...((params.filter.and as unknown[] | undefined) ?? []),
      lteFilter(params.timestamp),
      ...cursors.map(lteFilter),
    ],
  };

  if (cursors.length === 0) {
    return {
      ...params.body,
      filter,
      limit: params.limit,
      orderBy: params.orderBy,
    };
  }

  return { filter, limit: LOAD_MORE_LIMIT, orderBy: params.orderBy };
};

export const mergeActivityPages = (pages: ActivitiesPage[]) => {
  const seen = new Set<string>();
  const merged: Activity[] = [];
  pages.forEach((page, index) => {
    page.rows.forEach((row) => {
      if (index === 0 || !seen.has(row.id)) {
        merged.push(row);
      }
      seen.add(row.id);
    });
  });
  return merged;
};

export const useActivitiesQuery = (params: Ref<ActivitiesQueryParams | null>) =>
  useInfiniteQuery({
    queryKey: computed(() => queryKeys.activities.list(params.value)),
    queryFn: ({ pageParam }): Promise<ActivitiesPage> =>
      ActivityService.query(buildBody(params.value as ActivitiesQueryParams, pageParam)),
    initialPageParam: [] as string[],
    getNextPageParam: (lastPage, allPages, lastPageParam) => {
      const activities = mergeActivityPages(allPages);
      const cursor = activities.at(-1)?.timestamp;
      if (!cursor || activities.length >= allPages[0].count || lastPage.rows.length === 0) {
        return undefined;
      }
      return [...lastPageParam, cursor];
    },
    enabled: computed(() => !!params.value),
    retry: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
  });
