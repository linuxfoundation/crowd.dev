// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import type { MultiSelectAsyncFilterOption } from '@/shared/modules/filters/types/filterTypes/MultiSelectAsyncFilterConfig';
import type { Project } from '@/modules/lf/segments/types/Segments';

export interface OrganizationsFilterData {
  selected?: MultiSelectAsyncFilterOption[];
}

export interface ProjectsFilterData {
  options: Project[];
}
