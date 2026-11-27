// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import type {
  EventMappingPlatform,
  EventMappingPlatformConfig,
} from '@/modules/admin/modules/event-mapping/types/EventMapping';

export const eventMappingPlatforms: Record<EventMappingPlatform, EventMappingPlatformConfig> = {
  sessionize: { label: 'Sessionize', dotClass: 'bg-green-500' },
};
