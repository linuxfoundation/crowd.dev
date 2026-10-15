// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { computed } from 'vue';
import type { Ref } from 'vue';
import { keepPreviousData, useQuery } from '@tanstack/vue-query';
import { MemberService } from '@/modules/member/member-service';
import { queryKeys } from '@/shared/queries/keys';
import type { EntityListKeyParams } from '@/shared/queries/keys';
import type { Member } from '@/modules/member/types/Member';
import type { Pagination } from '@/shared/types/Pagination';

export type MembersListParams = Omit<EntityListKeyParams, 'projectGroupId'>;

export function useMembersQuery(
  params: Ref<MembersListParams>,
  projectGroupId: Ref<string | undefined>,
  enabled: boolean,
) {
  const body = computed<MembersListParams>(() => ({
    search: params.value.search,
    filter: params.value.filter,
    offset: params.value.offset,
    limit: params.value.limit,
    orderBy: params.value.orderBy,
    segments: params.value.segments,
  }));

  return useQuery({
    queryKey: computed(() => queryKeys.members.list({ projectGroupId: projectGroupId.value, ...body.value })),
    queryFn: (): Promise<Pagination<Member>> => MemberService.listMembers(body.value),
    enabled,
    placeholderData: keepPreviousData,
  });
}
