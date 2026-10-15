// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { defineStore } from 'pinia';
import { ref } from 'vue';
import type { AxiosError } from 'axios';
import { MemberService } from '@/modules/member/member-service';
import Errors from '@/shared/error/errors';
import { ToastStore } from '@/shared/message/notification';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import { useContributorStore } from '@/modules/contributor/store/contributor.store';
import {
  buildBulkUpdatePayload,
  type BulkAttributesMember,
  type BulkAttributesToSave,
} from '@/modules/member/helpers/bulk-attributes';
import { customAttributesService } from '@/shared/modules/filters/services/custom-attributes.service';
import type { FilterCustomAttribute } from '@/shared/modules/filters/types/FilterCustomAttribute';
import type { Filter, FilterConfig } from '@/shared/modules/filters/types/FilterConfig';
import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';
import type { Pagination } from '@/shared/types/Pagination';
import type { Member } from '@/modules/member/types/Member';
import allMembers from '@/modules/member/config/saved-views/views/all-members';

const { buildFilterFromAttributes } = customAttributesService();

let lastRequestId = 0;

const refetchCustomAttributes = (): Promise<void> => MemberService.fetchCustomAttributes()
  .then(() => undefined)
  .catch((error: unknown) => {
    Errors.handle(error);
  });

export const useMemberStore = defineStore('member', () => {
  const filters = ref<Filter>({ ...allMembers.config });
  const savedFilterBody = ref<ApiFilter>({});
  const customAttributes = ref<FilterCustomAttribute[]>([]);
  const customAttributesFilter = ref<Record<string, FilterConfig>>({});
  const members = ref<Member[]>([]);
  const totalMembers = ref(0);
  const selectedMembers = ref<Member[]>([]);
  const toMergeMember = ref<{ id: string; segmentId: string } | null>(null);

  function fetchMembers({ body = {}, reload = false }: { body?: ApiFilter, reload?: boolean }): Promise<Pagination<Member>> {
    const mappedBody = reload ? { ...savedFilterBody.value, ...body } : body;
    selectedMembers.value = [];
    const currentRequestId = new Date().getTime();
    lastRequestId = currentRequestId;
    return MemberService.listMembers(mappedBody)
      .then((data: Pagination<Member>) => {
        if (lastRequestId === currentRequestId) {
          members.value = data.rows;
          totalMembers.value = data.count;
          savedFilterBody.value = mappedBody;
          return Promise.resolve(data);
        }
        return Promise.reject(data);
      })
      .catch((err) => {
        if (lastRequestId === currentRequestId) {
          members.value = [];
          totalMembers.value = 0;
        }
        return Promise.reject(err);
      });
  }

  function getMemberCustomAttributes(): Promise<Record<string, FilterConfig>> {
    return MemberService.fetchCustomAttributes()
      .then(({ rows }: { rows: FilterCustomAttribute[] }) => {
        customAttributes.value = rows;
        customAttributesFilter.value = buildFilterFromAttributes(rows);
        return Promise.resolve(customAttributesFilter.value);
      });
  }

  function fetchCustomAttributes(): Promise<void> {
    return refetchCustomAttributes();
  }

  function createCustomAttribute(values: { type: string; label: string }): Promise<unknown> {
    return MemberService.createCustomAttributes(values)
      .then((response: unknown) => {
        refetchCustomAttributes();
        return response;
      })
      .catch((error: AxiosError) => {
        // shortcut: assertion keeps main's throw on a response-less error. revisit: when error paths are reworked
        if (error.response!.status !== 500) {
          Errors.handle(error);
        }
        ToastStore.error('Custom Attributes could not be created');
        return null;
      });
  }

  function updateCustomAttribute(id: string, data: Record<string, unknown>): Promise<void> {
    return MemberService.updateCustomAttribute(id, data)
      .then(() => {
        refetchCustomAttributes();
      })
      .catch((error: unknown) => {
        Errors.handle(error);
      });
  }

  function destroyCustomAttribute(ids: string[]): Promise<void> {
    return MemberService.destroyCustomAttribute(ids)
      .then(() => {
        refetchCustomAttributes();
      })
      .catch((error: unknown) => {
        Errors.handle(error);
      });
  }

  function addToMergeMember(toMergeId: string, toMergeGrandParentId: string) {
    toMergeMember.value = {
      id: toMergeId,
      segmentId: toMergeGrandParentId,
    };
  }

  function removeToMergeMember() {
    toMergeMember.value = null;
  }

  function findMember(id: string): Promise<Contributor | null> {
    return useContributorStore().getContributor(id)
      .catch(async (error: unknown) => {
        Errors.handle(error);
        const { router } = await import('@/router');
        router.push({ name: 'member' });
        return null;
      });
  }

  async function bulkUpdateMembersAttribute(
    { members: membersToUpdate, attributesToSave }: { members: BulkAttributesMember[]; attributesToSave: BulkAttributesToSave },
  ): Promise<Member[] | undefined> {
    try {
      const payload = buildBulkUpdatePayload(membersToUpdate, attributesToSave);
      const updatedMembers: Member[] = await MemberService.updateBulk(payload);
      ToastStore.success('Attribute updated successfully');
      return updatedMembers;
    } catch (error) {
      Errors.handle(error);
      ToastStore.error('There was an error updating attribute');
      return undefined;
    }
  }

  return {
    filters,
    savedFilterBody,
    customAttributes,
    customAttributesFilter,
    members,
    totalMembers,
    selectedMembers,
    toMergeMember,
    fetchMembers,
    getMemberCustomAttributes,
    fetchCustomAttributes,
    createCustomAttribute,
    updateCustomAttribute,
    destroyCustomAttribute,
    addToMergeMember,
    removeToMergeMember,
    findMember,
    bulkUpdateMembersAttribute,
  };
});
