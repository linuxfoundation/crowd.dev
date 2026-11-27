// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import { computed, type Ref } from 'vue';
import { useInfiniteQuery, useQueryClient } from '@tanstack/vue-query';
import { listEventMappingProjects } from '@/modules/admin/modules/event-mapping/services/event-mapping.api.service';
import type { EventMappingProjectsPage } from '@/modules/admin/modules/event-mapping/types/EventMapping';
import { queryKeys } from '@/shared/queries/keys';

const PAGE_SIZE = 50;

export const useEventMappingProjectsQuery = (search: Ref<string>, enabled: Ref<boolean>) => {
  const queryClient = useQueryClient();

  const query = useInfiniteQuery({
    queryKey: computed(() => queryKeys.eventMappingProjects.search(search.value)),
    queryFn: ({ pageParam }): Promise<EventMappingProjectsPage> =>
      listEventMappingProjects({ search: search.value, limit: PAGE_SIZE, offset: pageParam }),
    getNextPageParam: (lastPage, pages) => {
      const loaded = pages.reduce((sum, page) => sum + page.rows.length, 0);
      return loaded < lastPage.total ? loaded : undefined;
    },
    initialPageParam: 0,
    enabled,
  });

  const projects = computed(() => query.data.value?.pages.flatMap(({ rows }) => rows) ?? []);

  const reset = () => queryClient.resetQueries({ queryKey: queryKeys.eventMappingProjects.all });

  return { ...query, projects, reset };
};
