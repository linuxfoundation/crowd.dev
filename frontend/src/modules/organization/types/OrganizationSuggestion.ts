// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import type { Organization } from '@/modules/organization/types/Organization';

export interface OrganizationMergeSuggestion {
  similarity: number;
  organizations: Organization[];
}

export interface FakeOrganizationSuggestion {
  organizationId: string;
  organization?: Partial<Organization>;
  displayName?: string;
  logo?: string | null;
  activityCount?: number;
}
