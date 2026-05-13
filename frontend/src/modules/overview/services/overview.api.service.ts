import type { QueryFunction } from '@tanstack/vue-query';
import { type ComputedRef, computed } from 'vue';
import { useInfiniteQuery, useQuery } from '@tanstack/vue-query';
import { TanstackKey } from '@/shared/types/tanstack';
import authAxios from '@/shared/axios/auth-axios';
import { Project } from '@/modules/lf/segments/types/Segments';
import { IntegrationProgress } from '@/modules/integration/types/IntegrationProgress';
import {
  DashboardMetrics,
  GlobalIntegrationStatusCount,
  IntegrationStatusResponse,
} from '../types/overview.types';

export interface GlobalIntegrationStatusCountQueryParams {
  platform: string | undefined
  segment?: string
}

export interface GlobalIntegrationIntegrationsQueryParams {
  platform: string | undefined
  status: string[]
  query: string
  limit: number
  segment?: string
}

export interface IntegrationProgressListQueryParams {
  segments?: string[]
}

export interface DashboardMetricsQueryParams {
  segment?: string
}

class OverviewApiService {
  fetchProjectById(params: ComputedRef<string>) {
    const queryKey = computed(() => [TanstackKey.PROJECT_BY_ID, params.value]);
    const queryFn = computed<QueryFunction<Project>>(() => this.fetchProjectByIdQueryFn(() => ({
      id: params.value,
    })));

    return useQuery<Project>({
      queryKey,
      queryFn,
    });
  }

  fetchProjectByIdQueryFn(
    query: () => Record<string, string | number | boolean | undefined | string[] | null>,
  ): QueryFunction<Project> {
    return () => authAxios
      .get(`/segment/${query().id}`, {
        params: {
          segments: [query().id],
        },
      })
      .then((res) => res.data);
  }

  fetchGlobalIntegrationStatusCount(params: ComputedRef<GlobalIntegrationStatusCountQueryParams>) {
    const queryKey = computed(() => [
      TanstackKey.GLOBAL_INTEGRATION_STATUS_COUNT,
      params.value.platform,
      params.value.segment,
    ]);
    const queryFn = computed<QueryFunction<GlobalIntegrationStatusCount[]>>(() => this.fetchGlobalIntegrationStatusCountQueryFn(() => ({
      platform: params.value.platform,
      segment: params.value.segment,
    })));

    return useQuery<GlobalIntegrationStatusCount[]>({
      queryKey,
      queryFn,
    });
  }

  fetchGlobalIntegrationStatusCountQueryFn(
    query: () => Record<string, string | number | boolean | undefined | string[] | null>,
  ): QueryFunction<GlobalIntegrationStatusCount[]> {
    return () => authAxios
      .get('/integration/global/status', {
        params: query(),
      })
      .then((res) => res.data);
  }

  fetchGlobalIntegrations(params: ComputedRef<GlobalIntegrationIntegrationsQueryParams>) {
    const queryKey = computed(() => [
      TanstackKey.GLOBAL_INTEGRATIONS,
      params.value.platform,
      params.value.status,
      params.value.query,
      params.value.limit,
      params.value.segment,
    ]);
    const queryFn = computed<QueryFunction<IntegrationStatusResponse>>(() => this.fetchGlobalIntegrationsQueryFn(() => ({
      ...params.value,
    })));

    return useInfiniteQuery<
      IntegrationStatusResponse,
      Error,
      IntegrationStatusResponse,
      readonly unknown[],
      number
    >({
      queryKey,
      // @ts-expect-error - TanStack Query type inference issue with Vue
      queryFn,
      getNextPageParam: this.getNextPageIntegrationsParam,
      initialPageParam: 0,
    });
  }

  fetchGlobalIntegrationsQueryFn(
    query: () => Record<string, string | number | boolean | undefined | string[] | null>,
  ): QueryFunction<IntegrationStatusResponse> {
    return ({ pageParam = 0 }) => authAxios
      .get('/integration/global', {
        params: { offset: pageParam, ...query() },
      })
      .then((res) => res.data);
  }

  getNextPageIntegrationsParam(lastPage: IntegrationStatusResponse): number | undefined {
    const nextPage = lastPage.offset + lastPage.limit;
    const totalRows = lastPage.count;
    return nextPage < totalRows ? nextPage : undefined;
  }

  fetchDashboardMetrics(params: ComputedRef<DashboardMetricsQueryParams>) {
    const queryKey = computed(() => [TanstackKey.DASHBOARD_METRICS, params.value.segment]);
    const queryFn = computed<QueryFunction<DashboardMetrics>>(() => this.fetchDashboardMetricsQueryFn(() => ({
      ...params.value,
    })));

    return useQuery<DashboardMetrics>({
      queryKey,
      queryFn,
    });
  }

  fetchDashboardMetricsQueryFn(
    query: () => Record<string, string | number | boolean | undefined | string[] | null>,
  ): QueryFunction<DashboardMetrics> {
    return () => authAxios
      .get('/dashboard/metrics', {
        params: query(),
      })
      .then((res) => res.data);
  }

  fetchIntegrationProgressList(params: ComputedRef<IntegrationProgressListQueryParams>) {
    const queryKey = computed(() => [TanstackKey.INTEGRATION_PROGRESS_LIST, params.value.segments]);
    const queryFn = computed<QueryFunction<IntegrationProgress[]>>(() => this.fetchIntegrationProgressListQueryFn(() => ({
      segments: params.value.segments,
    })));

    return useQuery<IntegrationProgress[]>({
      queryKey,
      queryFn,
      enabled: !!params.value.segments,
    });
  }

  fetchIntegrationProgressListQueryFn(
    query: () => Record<string, string | number | boolean | undefined | string[] | null>,
  ): QueryFunction<IntegrationProgress[]> {
    return () => authAxios
      .post(
        '/integration/progress/list',
        {
          segments: query().segments,
        },
      )
      .then((res) => res.data);
  }
}

export const OVERVIEW_API_SERVICE = new OverviewApiService();
