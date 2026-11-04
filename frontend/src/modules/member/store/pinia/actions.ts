import type { AxiosError } from 'axios';
import { MemberService } from '@/modules/member/member-service';
import Errors from '@/shared/error/errors';
import { ToastStore } from '@/shared/message/notification';
import { customAttributesService } from '@/shared/modules/filters/services/custom-attributes.service';
import type { FilterCustomAttribute } from '@/shared/modules/filters/types/FilterCustomAttribute';
import type { FilterConfig } from '@/shared/modules/filters/types/FilterConfig';
import type { MemberState } from '@/modules/member/store/pinia/state';
import type { Pagination } from '@/shared/types/Pagination';
import type { Member } from '@/modules/member/types/Member';
import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';

const { buildFilterFromAttributes } = customAttributesService();

let lastRequestId = 0;

const fetchCustomAttributes = (): Promise<void> =>
  MemberService.fetchCustomAttributes()
    .then(() => undefined)
    .catch((error: unknown) => {
      Errors.handle(error);
    });

export default {
  fetchMembers(
    this: MemberState,
    { body = {}, reload = false }: { body?: ApiFilter; reload?: boolean },
  ): Promise<Pagination<Member>> {
    const mappedBody = reload ? { ...this.savedFilterBody, ...body } : body;
    this.selectedMembers = [];
    const currentRequestId = new Date().getTime();
    lastRequestId = currentRequestId;
    return MemberService.listMembers(mappedBody)
      .then((data: Pagination<Member>) => {
        if (lastRequestId === currentRequestId) {
          this.members = data.rows;
          this.totalMembers = data.count;
          this.savedFilterBody = mappedBody;
          return Promise.resolve(data);
        }
        return Promise.reject(data);
      })
      .catch((err) => {
        if (lastRequestId === currentRequestId) {
          this.members = [];
          this.totalMembers = 0;
        }
        return Promise.reject(err);
      });
  },
  getMemberCustomAttributes(this: MemberState): Promise<Record<string, FilterConfig>> {
    return MemberService.fetchCustomAttributes().then(
      ({ rows }: { rows: FilterCustomAttribute[] }) => {
        this.customAttributes = rows;
        this.customAttributesFilter = buildFilterFromAttributes(rows);
        return Promise.resolve(this.customAttributesFilter);
      },
    );
  },
  fetchCustomAttributes(): Promise<void> {
    return fetchCustomAttributes();
  },
  createCustomAttribute(values: { type: string; label: string }): Promise<unknown> {
    return MemberService.createCustomAttributes(values)
      .then((response: unknown) => {
        fetchCustomAttributes();
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
  },
  updateCustomAttribute(id: string, data: Record<string, unknown>): Promise<void> {
    return MemberService.updateCustomAttribute(id, data)
      .then(() => {
        fetchCustomAttributes();
      })
      .catch((error: unknown) => {
        Errors.handle(error);
      });
  },
  destroyCustomAttribute(ids: string[]): Promise<void> {
    return MemberService.destroyCustomAttribute(ids)
      .then(() => {
        fetchCustomAttributes();
      })
      .catch((error: unknown) => {
        Errors.handle(error);
      });
  },
  addToMergeMember(this: MemberState, toMergeId: string, toMergeGrandParentId: string) {
    this.toMergeMember = {
      id: toMergeId,
      segmentId: toMergeGrandParentId,
    };
  },

  removeToMergeMember(this: MemberState) {
    this.toMergeMember = null;
  },
};
