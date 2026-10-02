import { MemberService } from '@/modules/member/member-service';
import { customAttributesService } from '@/shared/modules/filters/services/custom-attributes.service';
import type { FilterCustomAttribute } from '@/shared/modules/filters/types/FilterCustomAttribute';
import type { FilterConfig } from '@/shared/modules/filters/types/FilterConfig';
import type { MemberState } from '@/modules/member/store/pinia/state';
import type { Pagination } from '@/shared/types/Pagination';
import type { Member } from '@/modules/member/types/Member';

const { buildFilterFromAttributes } = customAttributesService();

let lastRequestId = 0;

export default {
  fetchMembers(this: MemberState, { body = {}, reload = false } :{ body?: any, reload?: boolean }): Promise<Pagination<Member>> {
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
    return MemberService.fetchCustomAttributes()
      .then(({ rows }: {rows: FilterCustomAttribute[]}) => {
        this.customAttributes = rows;
        this.customAttributesFilter = buildFilterFromAttributes(rows);
        return Promise.resolve(this.customAttributesFilter);
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
