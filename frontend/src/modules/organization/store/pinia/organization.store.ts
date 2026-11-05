// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { defineStore, storeToRefs } from 'pinia';
import { ref } from 'vue';
import type { Pagination } from '@/shared/types/Pagination';
import type { Filter } from '@/shared/modules/filters/types/FilterConfig';
import type { Organization, OrganizationQueryBody } from '@/modules/organization/types/Organization';
import { OrganizationService } from '@/modules/organization/organization-service';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { MergeActionsService } from '@/shared/modules/merge/services/merge-actions.service';
import type { MergeAction } from '@/shared/modules/merge/types/MemberActions';
import allOrganizations from '@/modules/organization/config/saved-views/views/all-organizations';

let lastRequestId = 0;

export const useOrganizationStore = defineStore('organization', () => {
  const filters = ref<Filter>({ ...allOrganizations.config });
  const savedFilterBody = ref<OrganizationQueryBody>({});
  const organizations = ref<Organization[]>([]);
  const organization = ref<Organization | null>(null);
  const selectedOrganizations = ref<Organization[]>([]);
  const totalOrganizations = ref(0);
  const mergedOrganizations = ref<{ [key: string]: string }>({});
  const toMergeOrganizations = ref<{ originalId: string | null; toMergeId: string | null }>({
    originalId: null,
    toMergeId: null,
  });

  function fetchOrganizations({
    body = {},
    reload = false,
  }: {
    body?: OrganizationQueryBody;
    reload?: boolean;
  }): Promise<Pagination<Organization>> {
    const mappedBody = reload ? { ...savedFilterBody.value, ...body } : body;
    selectedOrganizations.value = [];
    const currentRequestId = new Date().getTime();
    lastRequestId = currentRequestId;
    return OrganizationService.query(mappedBody)
      .then((data: Pagination<Organization>) => {
        if (lastRequestId === currentRequestId) {
          organizations.value = data.rows;
          totalOrganizations.value = data.count;
          savedFilterBody.value = mappedBody;
          return Promise.resolve(data);
        }
        return Promise.reject(data);
      })
      .catch((err: Error) => {
        if (lastRequestId === currentRequestId) {
          organizations.value = [];
          totalOrganizations.value = 0;
        }
        return Promise.reject(err);
      });
  }

  function getOrganizationMergeActions(id: string): Promise<MergeAction[]> {
    return MergeActionsService.list(id, 'org').then((mergeActions) => {
      // shortcut: cast keeps the spread of a possibly null organization (details onMounted reset races merge actions). revisit: when merge actions skip a null organization
      organization.value = {
        ...organization.value,
        activitySycning: mergeActions.length > 0 ? mergeActions[0] : null,
      } as Organization;
      return Promise.resolve(mergeActions);
    });
  }

  function fetchOrganization(id: string): Promise<Organization> {
    const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());
    return OrganizationService.find(id, [selectedProjectGroup.value?.id]).then(
      (fetched: Organization) => {
        organization.value = fetched;
        getOrganizationMergeActions(id);
        return Promise.resolve(fetched);
      },
    );
  }

  function updateOrganization(id: string, data: Partial<Organization>): Promise<Organization> {
    return OrganizationService.update(id, data).then((updated: Organization) => {
      organization.value = updated;
      return fetchOrganization(id);
    });
  }

  function addMergedOrganizations(primaryId: string, secondaryId: string) {
    mergedOrganizations.value[primaryId] = secondaryId;
  }

  function removeMergedOrganizations(primaryId: string) {
    delete mergedOrganizations.value[primaryId];
  }

  function addToMergeOrganizations(originalId: string, toMergeId: string) {
    toMergeOrganizations.value.originalId = originalId;
    toMergeOrganizations.value.toMergeId = toMergeId;
  }

  function removeToMergeOrganizations() {
    toMergeOrganizations.value = {
      originalId: null,
      toMergeId: null,
    };
  }

  function getOrganizations() {
    return organizations.value;
  }

  return {
    filters,
    savedFilterBody,
    organizations,
    organization,
    selectedOrganizations,
    totalOrganizations,
    mergedOrganizations,
    toMergeOrganizations,
    fetchOrganizations,
    fetchOrganization,
    getOrganizationMergeActions,
    updateOrganization,
    addMergedOrganizations,
    removeMergedOrganizations,
    addToMergeOrganizations,
    removeToMergeOrganizations,
    getOrganizations,
  };
});
