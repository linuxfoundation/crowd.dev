// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { computed } from 'vue';
import type { Ref } from 'vue';
import { keepPreviousData, useQuery } from '@tanstack/vue-query';
import { OrganizationService } from '@/modules/organization/organization-service';
import { queryKeys } from '@/shared/queries/keys';
import type { EntityListKeyParams } from '@/shared/queries/keys';
import type { Organization } from '@/modules/organization/types/Organization';
import type { Pagination } from '@/shared/types/Pagination';

export type OrganizationsListParams = Omit<EntityListKeyParams, 'projectGroupId'>;

export function useOrganizationsQuery(
  params: Ref<OrganizationsListParams>,
  projectGroupId: Ref<string | undefined>,
  enabled: boolean,
) {
  const body = computed<OrganizationsListParams>(() => ({
    search: params.value.search,
    filter: params.value.filter,
    offset: params.value.offset,
    limit: params.value.limit,
    orderBy: params.value.orderBy,
    segments: params.value.segments,
  }));

  return useQuery({
    queryKey: computed(() => queryKeys.organizations.list({ projectGroupId: projectGroupId.value, ...body.value })),
    queryFn: (): Promise<Pagination<Organization>> => OrganizationService.query(body.value),
    enabled,
    placeholderData: keepPreviousData,
  });
}
