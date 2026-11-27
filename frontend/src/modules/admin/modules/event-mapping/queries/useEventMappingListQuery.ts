// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import type { Ref } from 'vue';
import { useQuery } from '@tanstack/vue-query';
import { listEventMappings } from '@/modules/admin/modules/event-mapping/services/event-mapping.api.service';
import type { EventMappingList } from '@/modules/admin/modules/event-mapping/types/EventMapping';
import { queryKeys } from '@/shared/queries/keys';

export const useEventMappingListQuery = (enabled: Ref<boolean>) =>
  useQuery<EventMappingList, Error>({
    queryKey: queryKeys.eventMapping.all,
    queryFn: listEventMappings,
    enabled,
  });
