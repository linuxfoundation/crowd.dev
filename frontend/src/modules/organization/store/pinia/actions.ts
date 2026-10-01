import type { Pagination } from '@/shared/types/Pagination';
import type { OrganizationState } from '@/modules/organization/store/pinia/state';
import type { Organization } from '@/modules/organization/types/Organization';
import { OrganizationService } from '@/modules/organization/organization-service';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { MergeActionsService } from '@/shared/modules/merge/services/merge-actions.service';
import type { MergeAction } from '@/shared/modules/merge/types/MemberActions';

let lastRequestId = 0;

export default {
  fetchOrganizations(this: OrganizationState, {
    body = {},
    reload = false,
  }: { body?: any, reload?: boolean }): Promise<Pagination<Organization>> {
    const mappedBody = reload ? { ...this.savedFilterBody, ...body } : body;
    this.selectedOrganizations = [];
    const currentRequestId = new Date().getTime();
    lastRequestId = currentRequestId;
    return OrganizationService.query(mappedBody)
      .then((data: Pagination<Organization>) => {
        if (lastRequestId === currentRequestId) {
          this.organizations = data.rows;
          this.totalOrganizations = data.count;
          this.savedFilterBody = mappedBody;
          return Promise.resolve(data);
        }
        return Promise.reject(data);
      })
      .catch((err: Error) => {
        if (lastRequestId === currentRequestId) {
          this.organizations = [];
          this.totalOrganizations = 0;
        }
        return Promise.reject(err);
      });
  },
  fetchOrganization(id: string): Promise<Organization> {
    const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());
    return OrganizationService.find(id, [selectedProjectGroup.value?.id as string])
      .then((organization: Organization) => {
        this.organization = organization;
        this.getOrganizationMergeActions(id);
        return Promise.resolve(organization);
      });
  },
  getOrganizationMergeActions(id: string): Promise<MergeAction[]> {
    return MergeActionsService.list(id, 'org')
      .then((mergeActions) => {
        this.organization = {
          ...this.organization,
          // eslint-disable-next-line no-nested-ternary
          activitySycning: mergeActions.length > 0 ? mergeActions[0] : null,
        };
        return Promise.resolve(mergeActions);
      });
  },
  updateOrganization(id: string, data: Partial<Organization>): Promise<Organization> {
    const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());
    return OrganizationService.update(id, data, [selectedProjectGroup.value?.id as string])
      .then((organization: Organization) => {
        this.organization = organization;
        return this.fetchOrganization(id);
      });
  },
  addMergedOrganizations(this: OrganizationState, primaryId: string, secondaryId: string) {
    this.mergedOrganizations[primaryId] = secondaryId;
  },

  removeMergedOrganizations(this: OrganizationState, primaryId: string) {
    delete this.mergedOrganizations[primaryId];
  },

  addToMergeOrganizations(this: OrganizationState, originalId: string, toMergeId: string) {
    this.toMergeOrganizations.originalId = originalId;
    this.toMergeOrganizations.toMergeId = toMergeId;
  },

  removeToMergeOrganizations(this: OrganizationState) {
    this.toMergeOrganizations = {
      originalId: null,
      toMergeId: null,
    };
  },

  getOrganizations(this: OrganizationState) {
    return this.organizations;
  },
};
