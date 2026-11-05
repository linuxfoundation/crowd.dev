// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { TanstackKey } from '@/shared/types/tanstack';
import type {
  DashboardMetricsQueryParams,
  GlobalIntegrationIntegrationsQueryParams,
  GlobalIntegrationStatusCountQueryParams,
  IntegrationProgressListQueryParams,
} from '@/modules/overview/services/overview.api.service';
import type { ActivitiesQueryParams } from '@/modules/activity/queries/useActivitiesQuery';

export interface EntityListKeyParams {
  projectGroupId?: string;
  search: string;
  filter: unknown;
  offset: number;
  limit: number;
  orderBy: string;
  segments: string[];
}

const entityListKey = (key: TanstackKey, params: EntityListKeyParams) =>
  [
    key,
    params.projectGroupId,
    params.search,
    params.filter,
    params.offset,
    params.limit,
    params.orderBy,
    params.segments,
  ] as const;

export const queryKeys = {
  activities: {
    all: [TanstackKey.ACTIVITIES_LIST] as const,
    list: (params: ActivitiesQueryParams | null) => [TanstackKey.ACTIVITIES_LIST, params] as const,
  },
  members: {
    all: [TanstackKey.MEMBERS_LIST] as const,
    list: (params: EntityListKeyParams) => entityListKey(TanstackKey.MEMBERS_LIST, params),
    mergeSuggestionsCount: (projectGroupId?: string) =>
      [TanstackKey.MEMBER_MERGE_SUGGESTIONS_COUNT, projectGroupId] as const,
  },
  organizations: {
    all: [TanstackKey.ORGANIZATIONS_LIST] as const,
    list: (params: EntityListKeyParams) => entityListKey(TanstackKey.ORGANIZATIONS_LIST, params),
    mergeSuggestionsCount: (projectGroupId?: string) =>
      [TanstackKey.ORGANIZATION_MERGE_SUGGESTIONS_COUNT, projectGroupId] as const,
  },
  collections: {
    all: [TanstackKey.ADMIN_COLLECTIONS] as const,
    search: (search: string) => [TanstackKey.ADMIN_COLLECTIONS, search] as const,
  },
  insightsProjects: {
    all: [TanstackKey.ADMIN_INSIGHTS_PROJECTS] as const,
    search: (search: string) => [TanstackKey.ADMIN_INSIGHTS_PROJECTS, search] as const,
    detail: (id?: string) => [TanstackKey.ADMIN_INSIGHTS_PROJECTS, id] as const,
  },
  subProjects: {
    search: (search: string) => [TanstackKey.ADMIN_SUB_PROJECTS, search] as const,
  },
  projectGroups: {
    all: [TanstackKey.ADMIN_PROJECT_GROUPS] as const,
    search: (search: string) => [TanstackKey.ADMIN_PROJECT_GROUPS, search] as const,
    adminList: (isProjectAdmin: boolean, search: string) =>
      [TanstackKey.ADMIN_PROJECT_GROUPS, isProjectAdmin, search] as const,
    detail: (id?: string | null) => [TanstackKey.ADMIN_PROJECT_GROUPS, id] as const,
    integrationFoundations: [TanstackKey.ADMIN_PROJECT_GROUPS, 'integration-foundations'] as const,
  },
  overview: {
    projectById: (id: string) => [TanstackKey.PROJECT_BY_ID, id] as const,
    integrationStatusCount: (params: GlobalIntegrationStatusCountQueryParams) =>
      [TanstackKey.GLOBAL_INTEGRATION_STATUS_COUNT, params.platform, params.segment] as const,
    integrationsAll: [TanstackKey.GLOBAL_INTEGRATIONS] as const,
    integrationStatusCountAll: [TanstackKey.GLOBAL_INTEGRATION_STATUS_COUNT] as const,
    integrationProgressAll: [TanstackKey.INTEGRATION_PROGRESS_LIST] as const,
    integrations: (params: GlobalIntegrationIntegrationsQueryParams) =>
      [
        TanstackKey.GLOBAL_INTEGRATIONS,
        params.platform,
        params.status,
        params.query,
        params.limit,
        params.segment,
      ] as const,
    dashboardMetrics: (params: DashboardMetricsQueryParams) =>
      [TanstackKey.DASHBOARD_METRICS, params.segment] as const,
    integrationProgress: (params: IntegrationProgressListQueryParams) =>
      [TanstackKey.INTEGRATION_PROGRESS_LIST, params.segments] as const,
  },
};
