// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { type InvalidateQueryFilters, useQueryClient } from '@tanstack/vue-query';
import { queryKeys } from '@/shared/queries/keys';

type InvalidateFilters = Omit<InvalidateQueryFilters, 'queryKey'>;

export const useInvalidate = () => {
  const queryClient = useQueryClient();
  const invalidate = (queryKey: readonly unknown[], filters?: InvalidateFilters) => queryClient.invalidateQueries({
    queryKey,
    ...filters,
  });

  return {
    members: (filters?: InvalidateFilters) => invalidate(queryKeys.members.all, filters),
    collections: (filters?: InvalidateFilters) => invalidate(queryKeys.collections.all, filters),
    insightsProjects: (filters?: InvalidateFilters) => invalidate(queryKeys.insightsProjects.all, filters),
    projectGroups: (filters?: InvalidateFilters) => invalidate(queryKeys.projectGroups.all, filters),
    globalIntegrations: (filters?: InvalidateFilters) => invalidate(queryKeys.overview.integrationsAll, filters),
  };
};
