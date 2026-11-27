// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import type { EventMappingPlatform } from '@/modules/admin/modules/event-mapping/types/EventMapping';

export interface EventMappingPlatformConfig {
  label: string;
  dotClass: string;
}

export const eventMappingPlatforms: Record<EventMappingPlatform, EventMappingPlatformConfig> = {
  sessionize: { label: 'Sessionize', dotClass: 'bg-green-500' },
};
