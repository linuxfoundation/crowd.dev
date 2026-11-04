// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import { defineStore } from 'pinia';
import { computed, ref } from 'vue';
import { IntegrationService } from '@/modules/integration/integration-service';
import Errors from '@/shared/error/errors';
import type { Router } from 'vue-router';
import { getRouter } from '@/router/instance';
import { isCurrentDateAfterGivenWorkingDays } from '@/utils/date';
import { lfIntegrations } from '@/config/integrations';
import type { IntegrationConfig } from '@/config/integrations';
import { ToastStore } from '@/shared/message/notification';
import type { Integration } from '@/modules/admin/modules/integration/types/Integration';
import type { GitHubSettings } from '@/config/integrations/github-nango/types/GithubSettings';

const ERROR_BANNER_WORKING_DAYS_DISPLAY = 3;

export interface IntegrationRecord extends Integration<Record<string, unknown>> {
  platform: string;
  segmentId?: string;
  updatedAt?: string;
  lastProcessedAt?: string;
  loading?: boolean;
  isNango?: boolean;
}

export type IntegrationListItem = IntegrationRecord & Partial<IntegrationConfig>;

const getNangoStatus = (integration: IntegrationRecord) => {
  const settings = integration.settings as unknown as GitHubSettings;
  const mapped = settings.nangoMapping ? Object.values(settings.nangoMapping) : [];
  const allRepos = settings.orgs.map((org) => org.repos).flat();
  return mapped.length < allRepos.length ? 'in-progress' : 'done';
};

const withConfig = (integration: IntegrationRecord): IntegrationListItem => ({
  ...integration,
  ...lfIntegrations()[integration.platform],
});

export const useIntegrationStore = defineStore('integration', () => {
  const integrations = ref<Record<string, IntegrationRecord>>({});
  const allIds = ref<string[]>([]);
  const count = ref(0);
  const loading = ref(false);
  const loaded = ref(false);
  const segmentId = ref<string | null>(null);

  const loadingFetch = computed(() => loading.value);
  const loadingFind = computed(() => (id: string) => integrations.value[id]?.loading);
  const findById = computed(() => (id: string) => integrations.value[id]);

  const list = computed(() =>
    Object.keys(integrations.value).reduce<Record<string, IntegrationListItem>>((acc, key) => {
      acc[key] = withConfig(integrations.value[key]);
      return acc;
    }, {}),
  );

  const listByPlatform = computed(() =>
    Object.keys(integrations.value).reduce<Record<string, IntegrationListItem>>((acc, key) => {
      acc[integrations.value[key].platform] = withConfig(integrations.value[key]);
      return acc;
    }, {}),
  );

  const array = computed(() => allIds.value.map((id) => list.value[id]));
  const findByPlatform = computed(
    () => (platform: string) => array.value.find((w) => w.platform === platform),
  );
  const active = computed(() =>
    array.value.filter((i) => i.status === 'done' || i.status === 'in-progress'),
  );
  const activeList = computed(() =>
    active.value.reduce<Record<string, IntegrationListItem>>((acc, item) => {
      acc[item.platform] = item;
      return acc;
    }, {}),
  );
  const inProgress = computed(() => array.value.filter((i) => i.status === 'in-progress'));
  const withErrors = computed(() =>
    array.value.filter(
      (i) =>
        i.status === 'error' &&
        isCurrentDateAfterGivenWorkingDays(i.updatedAt, ERROR_BANNER_WORKING_DAYS_DISPLAY),
    ),
  );
  const withNoData = computed(() => array.value.filter((i) => i.status === 'no-data'));
  const needsReconnect = computed(() => array.value.filter((i) => i.status === 'needs-reconnect'));
  const hasRows = computed(() => count.value > 0);

  const upsert = (record: IntegrationRecord) => {
    integrations.value[record.id] = record;
    if (!allIds.value.includes(record.id)) {
      allIds.value.push(record.id);
      count.value += 1;
    }
  };

  const fetch = async (segments: string[] = []): Promise<void> => {
    try {
      loading.value = true;

      const response = await IntegrationService.list(null, null, null, null, segments);

      // Temporary workaround while two GitHub integrations are supported
      const rows: IntegrationRecord[] = response.rows.map((row: IntegrationRecord) => {
        if (row.platform === 'github-nango') {
          return {
            ...row,
            platform: 'github',
            isNango: true,
            status: getNangoStatus(row),
          };
        }
        return row;
      });

      const byId: Record<string, IntegrationRecord> = {};
      const ids: string[] = [];
      rows.forEach((integration) => {
        byId[integration.id] = integration;
        if (!ids.includes(integration.id)) {
          ids.push(integration.id);
        }
      });

      loading.value = false;
      segmentId.value =
        ((getRouter() as Router).currentRoute.value.params.id as string | undefined) ?? null;
      integrations.value = byId;
      allIds.value = ids;
      count.value = response.count;
      loaded.value = true;
    } catch (error) {
      Errors.handle(error);
      loading.value = false;
      count.value = 0;
    }
  };

  const find = async (id: string): Promise<void> => {
    try {
      if (integrations.value[id]) {
        integrations.value[id].loading = true;
      }
      const record: IntegrationRecord | null = await IntegrationService.find(id);
      if (!record) {
        return;
      }
      integrations.value[record.id] = record;
      integrations.value[record.id].loading = false;
      if (!allIds.value.includes(record.id)) {
        allIds.value.push(record.id);
      }
    } catch (error) {
      Errors.handle(error);
      if (integrations.value[id]) {
        integrations.value[id].loading = false;
      }
    }
  };

  const destroy = async (id: string): Promise<void> => {
    try {
      loading.value = true;

      await IntegrationService.destroyAll([id]);
      ToastStore.success('Integration was disconnected successfully');

      loading.value = false;
      delete integrations.value[id];
      allIds.value.splice(allIds.value.indexOf(id), 1);
      count.value -= 1;
      fetch();
    } catch (error) {
      Errors.handle(error);
      loading.value = false;
    }
  };

  return {
    integrations,
    allIds,
    count,
    loading,
    loaded,
    segmentId,
    loadingFetch,
    loadingFind,
    findById,
    findByPlatform,
    list,
    listByPlatform,
    array,
    active,
    activeList,
    inProgress,
    withErrors,
    withNoData,
    needsReconnect,
    hasRows,
    upsert,
    fetch,
    find,
    destroy,
  };
});
